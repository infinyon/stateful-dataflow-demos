use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
};

use anyhow::Result;
use tracing::info;

use sdf_common::{
    constants::DATAFLOW_STABLE_VERSION,
    version::{ApiVersion, SdfContextVersion},
};

use crate::{
    importer::resolver::DependencyResolver,
    metadata::{
        io::topic::{validate_topic_name, TopicValidationError, TopicValidationFailure},
        metadata::header::HeaderValidationError,
    },
    util::{
        merge::merge_types_and_states,
        config_error::{ConfigError, INDENT},
        operator_placement::OperatorPlacement,
        sdf_types_map::SdfTypesMap,
        validate::MetadataTypeValidationFailure,
    },
    wit::{
        dataflow::{DataflowDefinition, Header, Operations, PackageDefinition, State},
        metadata::{MetadataType, SdfType, SdfTypeOrigin},
        operator::OperatorType,
        package_interface::{PackageImport, StepInvocation},
    },
};

use super::{operations::ServiceValidationFailure, schedule_config::ScheduleValidationFailure};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DataflowDefinitionValidationFailure {
    pub errors: Vec<DataflowDefinitionValidationError>,
}

impl Display for DataflowDefinitionValidationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Dataflow Config failed validation\n")?;

        for error in &self.errors {
            writeln!(f, "{}", error.readable(1))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DataflowDefinitionValidationError {
    Meta(Vec<HeaderValidationError>),
    Type(MetadataTypeValidationFailure),
    Topic(TopicValidationFailure),
    Service(ServiceValidationFailure),
    DuplicateOperator(String),
    Schedule(ScheduleValidationFailure),
    UndefinedState {
        service_name: String,
        ref_state_name: String,
    },
    Versioning(DataflowDefinitionVersionError),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DataflowDefinitionVersionError {
    UnsupportedVersion(String),
    InvalidVersionFeature {
        version: String,
        feature: String,
        supported_version: String,
    },
    FailedToParseVersion(String),
}

impl ConfigError for DataflowDefinitionVersionError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::UnsupportedVersion(version) => {
                format!("{}Unsupported version: {}\n", indent, version)
            }
            Self::InvalidVersionFeature {
                version,
                feature,
                supported_version,
            } => {
                format!(
                    "{}Version {} does not support configuration: {}, supported version: {}\n",
                    indent, version, feature, supported_version
                )
            }
            Self::FailedToParseVersion(err) => {
                format!("{}Failed to parse version: {}\n", indent, err)
            }
        }
    }
}

impl ConfigError for DataflowDefinitionValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::Meta(errors) => {
                let mut res = format!("{}Header is invalid:\n", indent);

                for error in errors {
                    res.push_str(&error.readable(indents + 1));
                }

                res
            }
            Self::Type(failure) => failure.readable(indents),
            Self::Topic(failure) => failure.readable(indents),
            Self::Service(failure) => failure.readable(indents),
            Self::Schedule(failure) => failure.readable(indents),
            Self::DuplicateOperator(name) => {
                format!("{}Duplicate inline operator with name: {} was found, inline operators must have unique names\n", indent, name)
            }
            Self::UndefinedState {
                service_name,
                ref_state_name,
            } => {
                format!(
                    "{}State with name {} is referenced in service {} but not defined in the dataflow\n",
                    indent, ref_state_name, service_name
                )
            }
            Self::Versioning(err) => err.readable(indents),
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for DataflowDefinition {
    fn default() -> Self {
        Self {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Default::default(),
            imports: Default::default(),
            types: Default::default(),
            services: Default::default(),
            topics: Default::default(),
            dev: Default::default(),
            packages: Default::default(),
            schedule: Default::default(),
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for Header {
    fn default() -> Self {
        Self {
            name: Default::default(),
            version: Default::default(),
            namespace: Default::default(),
        }
    }
}

impl DataflowDefinition {
    /// readable name
    pub fn name(&self) -> String {
        self.meta.to_string()
    }

    pub fn has_custom_types(&self) -> bool {
        !self.types.is_empty() || self.services.iter().any(|s| !s.states.is_empty())
    }

    pub fn resolve_imports(&mut self, debug: bool) -> Result<()> {
        let dependency_resolver =
            DependencyResolver::build(self.imports.clone(), self.packages.clone(), debug)?;
        let package_configs = dependency_resolver.packages()?;

        self.merge_dependencies(&package_configs)?;

        for service in self.services.iter_mut() {
            service.import_operator_configs(&self.imports, &package_configs)?;
        }

        self.packages = package_configs;

        Ok(())
    }

    pub fn add_imported_operator(
        &mut self,
        function: StepInvocation,
        operator_type: OperatorType,
        operator_placement: OperatorPlacement,
        package_import: PackageImport,
    ) -> Result<()> {
        self.merge_package_import(package_import);

        let service = self.get_service_mut(&operator_placement.service_id)?;

        service.add_operator(operator_type, operator_placement, function)?;

        Ok(())
    }

    pub fn add_inline_operator(
        &mut self,
        function: StepInvocation,
        operator_type: OperatorType,
        operator_placement: OperatorPlacement,
    ) -> Result<()> {
        if function.code_info.code.is_none() {
            return Err(anyhow::anyhow!("inline operator must have code"));
        }

        let service = self.get_service_mut(&operator_placement.service_id)?;

        service.add_operator(operator_type, operator_placement, function)?;

        Ok(())
    }

    pub fn replace_inline_operator(
        &mut self,
        function: StepInvocation,
        operator_type: OperatorType,
        operator_placement: OperatorPlacement,
    ) -> Result<()> {
        if function.code_info.code.is_none() {
            return Err(anyhow::anyhow!("inline operator must have code"));
        }

        self.delete_operator(operator_placement.clone())?;
        self.add_inline_operator(function, operator_type, operator_placement)?;

        Ok(())
    }

    pub fn delete_operator(&mut self, operator_placement: OperatorPlacement) -> Result<()> {
        match self.get_service_mut(&operator_placement.service_id) {
            Ok(service) => service.delete_operator(operator_placement),
            Err(e) => Err(e),
        }
    }

    fn get_service_mut(&mut self, service_id: &str) -> Result<&mut Operations> {
        match self.services.iter_mut().find(|s| s.name == service_id) {
            Some(s) => Ok(s),
            None => Err(anyhow::anyhow!("Service with id {} not found", service_id)),
        }
    }

    pub fn merge_dependencies(&mut self, package_configs: &[PackageDefinition]) -> Result<()> {
        let mut all_types = self.types_map();
        let mut all_states = BTreeMap::new();

        merge_types_and_states(
            &mut all_types,
            &mut all_states,
            &self.imports,
            package_configs,
        )?;

        self.types = all_types
            .iter()
            .map(|(name, (ty, origin))| MetadataType {
                name: name.clone(),
                type_: ty.clone(),
                origin: origin.to_owned(),
            })
            .collect();

        Ok(())
    }

    pub fn validate(&self) -> Result<(), DataflowDefinitionValidationFailure> {
        info!("validating dataflow");
        let mut errors: Vec<DataflowDefinitionValidationError> = vec![];

        if let Err(validate_version_errors) = self.validate_version() {
            errors.push(DataflowDefinitionValidationError::Versioning(
                validate_version_errors,
            ));
        }

        if let Err(header_errors) = self.meta.validate() {
            errors.push(DataflowDefinitionValidationError::Meta(header_errors));
        }

        let types_map = self.types_map();

        for metadata_type in &self.types {
            if let Err(type_validation_failure) = metadata_type.validate(&types_map) {
                errors.push(DataflowDefinitionValidationError::Type(
                    type_validation_failure,
                ));
            }
        }

        // TODO!: need to have a discussion on whether wit definition for topics, states, types, etc..
        // should be maps or arrays in both dataflow and package configs. This tuple should not exist
        // since the Topic struct has a name field as well.
        for (topic_name, topic) in self.topics.iter() {
            if let Err(name_errors) = validate_topic_name(topic_name) {
                errors.push(DataflowDefinitionValidationError::Topic(
                    TopicValidationFailure {
                        name: topic_name.to_string(),
                        errors: vec![TopicValidationError::Name(name_errors)],
                    },
                ));
            }

            if let Err(topic_validation_failure) = topic.validate(&types_map) {
                errors.push(DataflowDefinitionValidationError::Topic(
                    topic_validation_failure,
                ));
            }
        }

        if let Some(schedule) = &self.schedule {
            for schedule_config in schedule {
                if let Err(schedule_validation_failure) = schedule_config.validate() {
                    errors.push(DataflowDefinitionValidationError::Schedule(
                        schedule_validation_failure,
                    ))
                }
            }
        }

        for service in &self.services {
            if let Err(service_validation_failure) =
                service.validate(&types_map, &self.topics, self.schedule.as_deref())
            {
                errors.push(DataflowDefinitionValidationError::Service(
                    service_validation_failure,
                ))
            }
        }

        if let Err(err) = self.validate_states() {
            errors.push(err);
        }

        if let Err(err) = self.validate_non_duplicates() {
            for name in err {
                errors.push(DataflowDefinitionValidationError::DuplicateOperator(name));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(DataflowDefinitionValidationFailure { errors })
        }
    }

    pub fn validate_version(&self) -> Result<(), DataflowDefinitionVersionError> {
        // only valid 0.5.0 and 0.6.0
        // schedule, only valid on 0.6.0
        let version = ApiVersion::from(&self.api_version)
            .map_err(|err| DataflowDefinitionVersionError::FailedToParseVersion(err.to_string()))?;

        if version.is_v5() {
            if let Some(schedule) = &self.schedule {
                if !schedule.is_empty() {
                    return Err(DataflowDefinitionVersionError::InvalidVersionFeature {
                        version: self.api_version.clone(),
                        feature: "schedule".to_string(),
                        supported_version: "0.6.0".to_string(),
                    });
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }

        if version.is_v6() {
            return Ok(());
        }

        Err(DataflowDefinitionVersionError::UnsupportedVersion(
            self.api_version.clone(),
        ))
    }

    pub fn types_map(&self) -> SdfTypesMap {
        SdfTypesMap {
            map: self
                .types
                .iter()
                .map(|ty| (ty.name.clone(), (ty.type_.clone(), ty.origin)))
                .chain(self.services.iter().flat_map(|service| {
                    service.states.iter().filter_map(|state| {
                        if let State::Typed(state) = state {
                            Some((
                                state.name.clone(),
                                (
                                    SdfType::KeyedState(state.type_.to_owned()),
                                    SdfTypeOrigin::Local,
                                ),
                            ))
                        } else {
                            None
                        }
                    })
                }))
                .collect(),
        }
    }

    pub fn api_version(&self) -> Result<ApiVersion> {
        ApiVersion::from(&self.api_version)
    }

    pub fn all_owned_states(&self) -> BTreeMap<String, State> {
        let states = self
            .services
            .iter()
            .flat_map(|service| service.states.iter().cloned());

        states.into_iter().fold(BTreeMap::new(), |mut acc, state| {
            if let State::Typed(ref state_ty) = state {
                acc.insert(state_ty.name.clone(), state);
            }

            acc
        })
    }

    pub fn validate_states(&self) -> Result<(), DataflowDefinitionValidationError> {
        let (typed_states, ref_states): (Vec<_>, Vec<_>) = self
            .services
            .iter()
            .flat_map(|service| service.states.iter().map(|state| (&service.name, state)))
            .partition(|(_, state)| matches!(state, State::Typed(_)));

        let mut typed_states_set = HashSet::new();
        for (service_name, state) in typed_states {
            if let State::Typed(state_ty) = state {
                let ref_state_name = format!("{}.{}", service_name, state_ty.name);
                typed_states_set.insert(ref_state_name);
            } else {
                unreachable!("state should be typed at this point");
            }
        }

        for (service_name, state) in ref_states {
            if let State::Reference(ref_state) = state {
                let ref_service = ref_state.ref_service.as_str();
                let ref_state_name = ref_state.name.as_str();
                let ref_state_name = format!("{}.{}", ref_service, ref_state_name);
                if !typed_states_set.contains(&ref_state_name) {
                    return Err(DataflowDefinitionValidationError::UndefinedState {
                        service_name: service_name.to_owned(),
                        ref_state_name,
                    });
                }
            }
        }

        Ok(())
    }
    pub fn merge_package_import(&mut self, package_import: PackageImport) {
        if let Some(existing_import) = self
            .imports
            .iter_mut()
            .find(|import| import.metadata == package_import.metadata)
        {
            existing_import.merge(&package_import);
        } else {
            self.imports.push(package_import);
        }
    }

    #[cfg(feature = "parser")]
    pub fn update_inline_operators(&mut self) -> Result<()> {
        for service in self.services.iter_mut() {
            service.update_inline_operators()?;
        }

        Ok(())
    }

    fn validate_non_duplicates(&self) -> Result<(), Vec<String>> {
        let mut duplicate_names = vec![];
        let mut op_names = HashSet::new();

        for service in &self.services {
            for source in &service.sources {
                for step in &source.steps {
                    if step.is_imported(&self.imports) {
                        continue;
                    }

                    if !op_names.insert(step.name().to_owned()) {
                        duplicate_names.push(step.name().to_owned());
                    }
                }
            }

            for sink in &service.sinks {
                for step in &sink.steps {
                    if step.is_imported(&self.imports) {
                        continue;
                    }

                    if !op_names.insert(step.name().to_owned()) {
                        duplicate_names.push(step.name().to_owned());
                    }
                }
            }

            for (step, _) in service.operators() {
                if step.is_imported(&self.imports) {
                    continue;
                }

                if !op_names.insert(step.uses.to_owned()) {
                    duplicate_names.push(step.uses.to_owned());
                }
            }
        }

        if duplicate_names.is_empty() {
            Ok(())
        } else {
            Err(duplicate_names)
        }
    }
}

#[cfg(test)]
mod test {

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use crate::{
        metadata::{
            dataflow::{
                dataflow_definition::{
                    DataflowDefinitionValidationError, DataflowDefinitionVersionError,
                },
                operations::{ServiceValidationError, ServiceValidationFailure},
            },
            io::topic::{TopicNameError, TopicValidationError, TopicValidationFailure},
            metadata::{header::HeaderValidationError, sdf_type::SdfTypeValidationError},
        },
        util::{
            operator_placement::OperatorPlacement,
            validate::{MetadataTypeValidationError, MetadataTypeValidationFailure},
        },
        wit::{
            dataflow::{
                DataflowDefinition, IoRef, IoType, Operations, PostTransforms, ScheduleConfig,
                Schedule, Topic, TransformOperator,
            },
            io::{SchemaSerDe, SerdeConverter, TopicSchema},
            metadata::{
                ArrowColumnKind, Header, MetadataType, NamedParameter, ObjectField, OutputType,
                Parameter, ParameterKind, SdfArrowColumn, SdfArrowRow, SdfKeyedState,
                SdfKeyedStateValue, SdfObject, SdfType, SdfTypeOrigin, SerdeConfig, TypeRef,
            },
            operator::{
                CodeInfo, CodeLang, OperatorType, PartitionOperator, Transforms, TumblingWindow,
                WatermarkConfig, Window, WindowKind, WindowProperties,
            },
            package_interface::{
                FunctionImport, PackageDefinition, PackageImport, StateTyped, StepInvocation,
            },
            states::{State, StateRef},
        },
    };

    fn first_package_definition() -> PackageDefinition {
        PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                namespace: "example".to_string(),
                name: "bank-update".to_string(),
                version: "0.1.0".to_string(),
            },
            imports: vec![PackageImport {
                metadata: Header {
                    namespace: "example".to_string(),
                    name: "bank".to_string(),
                    version: "0.1.0".to_string(),
                },
                types: vec!["bank-event".to_string(), "bank-account".to_string()],
                states: vec!["account-balance".to_string()],
                path: Some("../bank-types".to_string()),
                functions: vec![],
            }],
            functions: vec![(
                StepInvocation {
                    uses: "filter-positive-events".to_string(),
                    inputs: vec![NamedParameter {
                        name: "event".to_string(),
                        type_: TypeRef {
                            name: "bank-event".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    }],
                    ..Default::default()
                },
                OperatorType::Filter,
            )],
            dev: None,
            states: vec![],
            types: vec![],
        }
    }

    fn second_package_definition() -> PackageDefinition {
        PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                namespace: "example".to_string(),
                name: "bank".to_string(),
                version: "0.1.0".to_string(),
            },
            types: vec![
                MetadataType {
                    name: "bank-event".to_string(),
                    type_: SdfType::Object(SdfObject {
                        fields: vec![
                            ObjectField {
                                name: "name".to_string(),
                                type_: TypeRef {
                                    name: "string".to_string(),
                                },
                                optional: false,
                                serde_config: SerdeConfig {
                                    serialize: None,
                                    deserialize: None,
                                },
                            },
                            ObjectField {
                                name: "amount".to_string(),
                                type_: TypeRef {
                                    name: "float32".to_string(),
                                },
                                optional: false,
                                serde_config: SerdeConfig {
                                    serialize: None,
                                    deserialize: None,
                                },
                            },
                        ],
                    }),
                    origin: SdfTypeOrigin::Local,
                },
                MetadataType {
                    name: "bank-account".to_string(),
                    type_: SdfType::ArrowRow(SdfArrowRow {
                        columns: vec![
                            SdfArrowColumn {
                                name: "balance".to_string(),
                                type_: ArrowColumnKind::Float32,
                            },
                            SdfArrowColumn {
                                name: "name".to_string(),
                                type_: ArrowColumnKind::String,
                            },
                        ],
                    }),
                    origin: SdfTypeOrigin::Local,
                },
            ],
            states: vec![StateTyped {
                name: "account-balance".to_string(),
                type_: SdfKeyedState {
                    key: TypeRef {
                        name: "string".to_string(),
                    },
                    value: SdfKeyedStateValue::U32,
                },
            }],
            imports: vec![],
            functions: vec![],
            dev: None,
        }
    }

    fn dataflow() -> DataflowDefinition {
        DataflowDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                name: "example".to_string(),
                version: "0.1.0".to_string(),
                namespace: "example".to_string(),
            },
            imports: vec![
                PackageImport {
                    metadata: Header {
                        namespace: "example".to_string(),
                        name: "bank-update".to_string(),
                        version: "0.1.0".to_string(),
                    },
                    functions: vec![FunctionImport {
                        name: "filter-positive-events".to_string(),
                        alias: None,
                    }],
                    path: None,
                    types: vec![],
                    states: vec![],
                },
                PackageImport {
                    metadata: Header {
                        namespace: "example".to_string(),
                        name: "bank".to_string(),
                        version: "0.1.0".to_string(),
                    },
                    types: vec!["bank-event".to_string()],
                    functions: vec![],
                    states: vec![],
                    path: None,
                },
            ],
            types: vec![],
            services: vec![],
            topics: vec![],
            dev: None,
            packages: vec![],
            schedule: None,
        }
    }

    fn dataflow_b() -> DataflowDefinition {
        DataflowDefinition {
            meta: Header {
                name: "my-df".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            imports: vec![],
            types: vec![],
            services: vec![service()],
            topics: vec![],
            dev: None,
            packages: vec![],
            schedule: None,
        }
    }

    fn service() -> Operations {
        let sources = vec![IoRef {
            type_: IoType::Topic,
            id: "listing".to_string(),
            steps: vec![],
        }];
        let sinks = vec![IoRef {
            type_: IoType::Topic,
            id: "prospect".to_string(),
            steps: vec![],
        }];
        let transforms = Transforms {
            steps: vec![
                TransformOperator::FilterMap(StepInvocation {
                    uses: "listing_map_job".to_string(),
                    ..Default::default()
                }),
                TransformOperator::Map(StepInvocation {
                    uses: "job_map_prospect".to_string(),
                    ..Default::default()
                }),
            ],
        };

        let post_transforms = Some(PostTransforms::AssignTimestamp(Window {
            assign_timestamp: StepInvocation {
                uses: "assign_timestamp".to_string(),
                ..Default::default()
            },
            transforms: Transforms {
                steps: vec![TransformOperator::Map(StepInvocation {
                    uses: "prospect_map_prospect".to_string(),
                    ..Default::default()
                })],
            },
            partition: Some(PartitionOperator {
                assign_key: StepInvocation {
                    uses: "assign_key".to_string(),
                    ..Default::default()
                },
                transforms: Transforms {
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "prospect_map_prospect2".to_string(),
                        ..Default::default()
                    })],
                },
                update_state: None,
            }),
            flush: Some(StepInvocation {
                uses: "job_aggregate".to_string(),
                ..Default::default()
            }),
            properties: WindowProperties {
                kind: WindowKind::Tumbling(TumblingWindow {
                    duration: 3600000,
                    offset: 0,
                }),
                watermark_config: WatermarkConfig::default(),
            },
        }));

        Operations {
            name: "listing-to-prospect-op".to_string(),
            sources,
            sinks,
            transforms,
            post_transforms,
            states: vec![],
        }
    }

    #[test]
    fn test_merge_types_and_states() {
        let mut dataflow = dataflow();
        let package_configs = vec![first_package_definition(), second_package_definition()];
        assert_eq!(dataflow.types.len(), 0);

        dataflow.merge_dependencies(&package_configs).unwrap();

        assert_eq!(dataflow.types.first().unwrap().name, "bank-event");
    }

    #[test]
    fn test_validate_validates_states() {
        let mut dataflow = dataflow();
        dataflow.services = vec![Operations {
            name: "basic".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![State::Reference(StateRef {
                ref_service: "other".to_string(),
                name: "account-balance".to_string(),
            })],
        }];
        let res = dataflow
            .validate()
            .expect_err("should error for undefined state");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::UndefinedState {
                service_name: "basic".to_string(),
                ref_state_name: "other.account-balance".to_string()
            }));
    }

    #[test]
    fn test_validate_validates_metadata() {
        let mut dataflow = dataflow();
        dataflow.meta.name = "".to_string();

        let res = dataflow
            .validate()
            .expect_err("should error for empty name");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Meta(vec![
                HeaderValidationError::new("Name cannot be empty\n")
            ])));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Header is invalid:
        Name cannot be empty

"#
        );
    }

    #[test]
    fn test_validate_rejects_schedule_on_v5() {
        let mut dataflow = dataflow();
        dataflow.api_version = "0.5.0".to_string();
        dataflow.schedule = Some(vec![ScheduleConfig {
            name: "weekly".to_string(),
            schedule: Schedule::Cron("0 0 * * 0".to_string()),
        }]);

        let res = dataflow
            .validate()
            .expect_err("should error for schedule on v5");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Versioning(
                DataflowDefinitionVersionError::InvalidVersionFeature {
                    version: "0.5.0".to_string(),
                    feature: "schedule".to_string(),
                    supported_version: "0.6.0".to_string()
                }
            )));
    }
    #[test]
    fn test_validate_rejects_empty_type_names() {
        let mut dataflow = dataflow();

        dataflow.types = vec![MetadataType {
            name: "".to_string(),
            type_: SdfType::Object(SdfObject { fields: vec![] }),
            origin: SdfTypeOrigin::Local,
        }];

        let res = dataflow
            .validate()
            .expect_err("should error for empty type name");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Type(
                MetadataTypeValidationFailure {
                    name: "".to_string(),
                    errors: vec![MetadataTypeValidationError::EmptyName]
                }
            )));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Defined type `` is invalid:
        Name cannot be empty

"#
        );
    }

    #[test]
    fn test_validate_validates_types() {
        let mut dataflow = dataflow();

        dataflow.types = vec![MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Named(TypeRef {
                name: "foobar".to_string(),
            }),
            origin: SdfTypeOrigin::Local,
        }];

        let res = dataflow
            .validate()
            .expect_err("should error for invalid type reference");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Type(
                MetadataTypeValidationFailure {
                    name: "my-type".to_string(),
                    errors: vec![MetadataTypeValidationError::SdfType(
                        SdfTypeValidationError::InvalidRef("foobar".to_string())
                    )]
                }
            )));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Defined type `my-type` is invalid:
        Referenced type `foobar` not found in config or imported types

"#
        );
    }

    #[test]
    fn test_validate_validates_topic_names() {
        let mut dataflow = dataflow();

        dataflow.topics = vec![(
            "".to_string(),
            Topic {
                name: "my-topic".to_string(),
                schema: TopicSchema {
                    key: None,
                    value: SchemaSerDe {
                        converter: None,
                        type_: TypeRef {
                            name: "u8".to_string(),
                        },
                    },
                },
                consumer: None,
                producer: None,
                profile: None,
            },
        )];

        let res = dataflow
            .validate()
            .expect_err("should error for empty topic key");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Topic(
                TopicValidationFailure {
                    name: "".to_string(),
                    errors: vec![TopicValidationError::Name(vec![TopicNameError::Empty])]
                }
            )));

        assert!(res.to_string().contains(
            r#"Dataflow Config failed validation

    Topic `` is invalid:
        Topic name is invalid:
            Name cannot be empty
"#
        ));
    }

    #[test]
    fn test_validate_validates_topics() {
        let mut dataflow = dataflow();

        dataflow.topics = vec![(
            "my-topic".to_string(),
            Topic {
                name: "my-topic".to_string(),
                schema: TopicSchema {
                    key: None,
                    value: SchemaSerDe {
                        converter: Some(SerdeConverter::Raw),
                        type_: TypeRef {
                            name: "foobar".to_string(),
                        },
                    },
                },
                consumer: None,
                producer: None,
                profile: None,
            },
        )];

        let res = dataflow
            .validate()
            .expect_err("should error for invalid topic value type");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Topic(
                TopicValidationFailure {
                    name: "my-topic".to_string(),
                    errors: vec![TopicValidationError::InvalidValueRef("foobar".to_string())]
                }
            )));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Topic `my-topic` is invalid:
        Referenced type `foobar` not found in config or imported types

"#
        );
    }

    #[test]
    fn test_validate_validates_services() {
        let mut dataflow = dataflow_b();

        dataflow.services = vec![Operations {
            name: "".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        }];

        let res = dataflow
            .validate()
            .expect_err("should error for missing service name");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::Service(
                ServiceValidationFailure {
                    name: "".to_string(),
                    errors: vec![
                        ServiceValidationError::NameEmpty,
                        ServiceValidationError::NoSources
                    ]
                }
            )));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Service `` is invalid:
        Service name cannot be empty
        Service must have at least one source

"#
        );
    }

    #[test]
    fn test_validate_passes_valid_config() {
        let dataflow = dataflow();

        assert!(dataflow.validate().is_ok());
    }

    #[test]
    fn test_has_custom_types() {
        let mut dataflow = dataflow();
        assert!(!dataflow.has_custom_types());

        dataflow.types = vec![MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Object(SdfObject { fields: vec![] }),
            origin: SdfTypeOrigin::Local,
        }];

        assert!(dataflow.has_custom_types());
    }

    #[test]
    fn test_has_custom_types_with_states() {
        let mut dataflow = dataflow();
        assert!(!dataflow.has_custom_types());

        dataflow.services = vec![Operations {
            name: "my-service".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![State::Typed(StateTyped {
                name: "my-state".to_string(),
                type_: SdfKeyedState {
                    key: TypeRef {
                        name: "string".to_string(),
                    },
                    value: SdfKeyedStateValue::U32,
                },
            })],
        }];

        assert!(dataflow.has_custom_types());
    }

    #[test]
    fn test_validate_rejects_dataflows_with_operator_name_duplicated() {
        let mut dataflow = dataflow();

        dataflow.topics.push((
            "listing".to_string(),
            Topic {
                name: "listing".to_string(),
                schema: TopicSchema {
                    key: None,
                    value: SchemaSerDe {
                        converter: Some(crate::wit::io::SerdeConverter::Json),
                        type_: TypeRef {
                            name: "string".to_string(),
                        },
                    },
                },
                consumer: None,
                producer: None,
                profile: None,
            },
        ));

        dataflow.validate().expect("should validate first");

        dataflow.services.push(Operations {
            name: "my-op".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "listing".to_string(),
                steps: vec![],
            }],
            sinks: vec![],
            transforms: Transforms {
                steps: vec![
                    TransformOperator::Filter(StepInvocation {
                        uses: "duplicated-fn".to_string(),
                        inputs: vec![NamedParameter {
                            name: "cat".to_string(),
                            type_: TypeRef {
                                name: "string".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: OutputType::Ref(TypeRef {
                                name: "bool".to_string(),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    TransformOperator::Filter(StepInvocation {
                        uses: "duplicated-fn".to_string(),
                        inputs: vec![NamedParameter {
                            name: "cat".to_string(),
                            type_: TypeRef {
                                name: "string".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: OutputType::Ref(TypeRef {
                                name: "bool".to_string(),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                ],
            },
            ..Default::default()
        });

        let res = dataflow
            .validate()
            .expect_err("should error for duplicated operator name");

        assert!(res
            .errors
            .contains(&DataflowDefinitionValidationError::DuplicateOperator(
                "duplicated-fn".to_string()
            )));

        assert_eq!(
            res.to_string(),
            r#"Dataflow Config failed validation

    Duplicate inline operator with name: duplicated-fn was found, inline operators must have unique names

"#
        );
    }
    #[test]
    fn test_add_imported_operator() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(2),
            ..Default::default()
        };

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            inputs: vec![NamedParameter {
                name: "cat".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: OutputType::Ref(TypeRef {
                    name: "string".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        dataflow
            .add_imported_operator(
                function.clone(),
                OperatorType::Map,
                operator_placement,
                package_import(),
            )
            .expect("Failed to add imported operator");

        let result_operator = dataflow.services.first().unwrap().transforms.steps[2].clone();

        assert_eq!(result_operator, TransformOperator::Map(function));
        assert_eq!(
            *dataflow
                .imports
                .first()
                .expect("Should be a package import"),
            package_import()
        );
    }

    #[test]
    fn test_add_imported_operator_with_index_out_of_bounds() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(3),
            ..Default::default()
        };

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            ..Default::default()
        };

        let res = dataflow.add_imported_operator(
            function,
            OperatorType::Map,
            operator_placement,
            package_import(),
        );

        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains(
            "cannot insert operator into transforms block, index is out of bounds, len = 2"
        ))
    }

    #[test]
    fn test_add_imported_operator_merges_package_import() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(2),
            ..Default::default()
        };

        // import an operator
        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            ..Default::default()
        };

        dataflow
            .add_imported_operator(
                function,
                OperatorType::Map,
                operator_placement,
                package_import(),
            )
            .expect("Failed to add imported operator");

        // import another operator from the same package
        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(3),
            ..Default::default()
        };

        let second_function = StepInvocation {
            uses: "cat_map_dog".to_string(),
            ..Default::default()
        };

        dataflow
            .add_imported_operator(
                second_function,
                OperatorType::Map,
                operator_placement,
                package_import_b(),
            )
            .expect("Failed to add imported operator");

        assert_eq!(dataflow.imports.len(), 1);
        assert_eq!(
            *dataflow
                .imports
                .first()
                .expect("Should be a package import"),
            package_import_merged()
        );
    }

    #[test]
    fn test_add_inline_operator() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(2),
            ..Default::default()
        };

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            code_info: CodeInfo {
                lang: CodeLang::Rust,
                code: Some("fn map_cat(cat: Cat) -> Cat { cat }".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        dataflow
            .add_inline_operator(function, OperatorType::Map, operator_placement)
            .expect("Failed to add imported operator");

        let result_operator = dataflow.services.first().unwrap().transforms.steps[2].clone();

        assert_eq!(
            result_operator,
            TransformOperator::Map(StepInvocation {
                uses: "cat_map_cat".to_string(),
                code_info: CodeInfo {
                    lang: CodeLang::Rust,
                    code: Some("fn map_cat(cat: Cat) -> Cat { cat }".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_add_inline_operator_has_no_code() {
        let mut dataflow = dataflow();

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            code_info: CodeInfo {
                lang: CodeLang::Rust,
                code: None,
                ..Default::default()
            },
            ..Default::default()
        };

        let res =
            dataflow.add_inline_operator(function, OperatorType::Map, OperatorPlacement::default());

        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("inline operator must have code"))
    }

    #[test]
    fn test_replace_inline_operator() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(2),
            ..Default::default()
        };

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            code_info: CodeInfo {
                lang: CodeLang::Rust,
                code: Some("fn map_cat(cat: Cat) -> Cat { cat }".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let new_function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            code_info: CodeInfo {
                lang: CodeLang::Rust,
                code: Some("fn map_dog(dog: Dog) -> Dog { dog }".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        dataflow
            .add_inline_operator(function, OperatorType::Map, operator_placement.clone())
            .expect("Failed to add imported operator");

        dataflow
            .replace_inline_operator(new_function, OperatorType::FilterMap, operator_placement)
            .expect("Failed to replace inline operator");

        let result_operator = dataflow.services.first().unwrap().transforms.steps[2].clone();

        assert_eq!(
            result_operator,
            TransformOperator::FilterMap(StepInvocation {
                uses: "cat_map_cat".to_string(),
                code_info: CodeInfo {
                    lang: CodeLang::Rust,
                    code: Some("fn map_dog(dog: Dog) -> Dog { dog }".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_replace_inline_operator_has_no_code() {
        let mut dataflow = dataflow();

        let function = StepInvocation {
            uses: "cat_map_cat".to_string(),
            code_info: CodeInfo {
                lang: CodeLang::Rust,
                code: None,
                ..Default::default()
            },
            ..Default::default()
        };

        let res = dataflow.replace_inline_operator(
            function,
            OperatorType::Map,
            OperatorPlacement::default(),
        );

        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("inline operator must have code"))
    }

    #[test]
    fn test_delete_operator() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(1),
            ..Default::default()
        };

        let _ = dataflow.delete_operator(operator_placement);

        let steps = &dataflow.services.first().unwrap().transforms.steps;
        let remaining_op = steps.first().unwrap();
        let function = match remaining_op {
            TransformOperator::FilterMap(f) => f,
            _ => panic!("Expected FilterMap operator"),
        };

        assert_eq!(steps.len(), 1);
        assert_eq!(function.uses, "listing_map_job");
    }

    #[test]
    fn test_delete_operator_fails_when_service_does_not_exist() {
        let mut dataflow = dataflow_b();

        let operator_placement = OperatorPlacement {
            service_id: "my-missing-service".to_string(),
            transforms_index: Some(1),
            ..Default::default()
        };

        let res = dataflow.delete_operator(operator_placement);

        assert_eq!(
            res.unwrap_err().to_string(),
            "Service with id my-missing-service not found"
        );
    }

    #[test]
    fn test_merge_package_import_merges_repeat_imports() {
        let mut dataflow = dataflow_b();

        dataflow.merge_package_import(package_import());
        dataflow.merge_package_import(package_import_b());

        assert_eq!(
            *dataflow
                .imports
                .first()
                .expect("Should be a package import"),
            package_import_merged()
        );
    }

    #[test]
    fn test_merge_package_import_appends_new_imports() {
        let mut dataflow = dataflow_b();

        let next_version_import = PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.1".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string(), "dog".to_string()],
            states: vec![],
            functions: vec![FunctionImport {
                name: "cat_map_dog".to_string(),
                alias: None,
            }],
        };

        dataflow.merge_package_import(package_import());
        dataflow.merge_package_import(next_version_import.clone());

        assert_eq!(dataflow.imports.len(), 2);
        assert_eq!(
            *dataflow
                .imports
                .first()
                .expect("Should be a package import"),
            package_import()
        );
        assert_eq!(dataflow.imports[1], next_version_import);
    }

    fn package_import() -> PackageImport {
        PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string()],
            states: vec![],
            functions: vec![FunctionImport {
                name: "cat_map_cat".to_string(),
                alias: None,
            }],
        }
    }

    fn package_import_b() -> PackageImport {
        PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string(), "dog".to_string()],
            states: vec![],
            functions: vec![FunctionImport {
                name: "cat_map_dog".to_string(),
                alias: None,
            }],
        }
    }

    fn package_import_merged() -> PackageImport {
        PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string(), "dog".to_string()],
            states: vec![],
            functions: vec![
                FunctionImport {
                    name: "cat_map_cat".to_string(),
                    alias: None,
                },
                FunctionImport {
                    name: "cat_map_dog".to_string(),
                    alias: None,
                },
            ],
        }
    }
}
