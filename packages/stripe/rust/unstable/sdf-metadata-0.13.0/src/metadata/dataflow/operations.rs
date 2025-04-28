use std::collections::BTreeMap;

use anyhow::{Result, anyhow};

use crate::{
    importer::{function::imported_operator_config, states::inject_states},
    metadata::{io::topic::KVSchemaType, operator::transforms::validate_transforms_steps},
    util::{
        config_error::{ConfigError, INDENT},
        operator_placement::OperatorPlacement,
        sdf_types_map::SdfTypesMap,
        validate::validate_all,
        validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::{
            IoType, Operations, PackageDefinition, PackageImport, PostTransforms, ScheduleConfig,
            Topic,
        },
        operator::StepInvocation,
        package_interface::OperatorType,
    },
};

use super::io_ref::IoRefValidationFailure;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ServiceValidationFailure {
    pub name: String,
    pub errors: Vec<ServiceValidationError>,
}

impl ConfigError for ServiceValidationFailure {
    fn readable(&self, indents: usize) -> String {
        let mut result = format!(
            "{}Service `{}` is invalid:\n",
            INDENT.repeat(indents),
            self.name
        );

        for error in &self.errors {
            result.push_str(&error.readable(indents + 1));
        }

        result
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ServiceValidationError {
    NameEmpty,
    MissingSourceTopic(String),
    NoSources,
    InvalidSource(IoRefValidationFailure),
    InvalidSink(IoRefValidationFailure),
    SourceTypeMismatch(String),
    SinkTypeMismatch(String),
    InvalidState(ValidationError),
    InvalidTransformsSteps(ValidationFailure),
    InvalidPostTransforms(ValidationFailure),
}

impl ConfigError for ServiceValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::NameEmpty => format!(
                "{}Service name cannot be empty\n",
                indent
            ),
            Self::MissingSourceTopic(topic) => format!(
                "{}Source topic `{}` not found\n",
                indent,
                topic
            ),
            Self::NoSources => format!(
                "{}Service must have at least one source\n",
                indent
            ),
            Self::InvalidSource(error) => format!(
                "{}Source `{}` is invalid:\n{}",
                indent,
                error.name,
                error.readable(indents + 1),
            ),
            Self::InvalidSink(error) => format!(
                "{}Sink `{}` is invalid:\n{}",
                indent,
                error.name,
                error.readable(indents + 1),
            ),
            Self::SourceTypeMismatch(types) => format!(
                "{}Sources for service must be identical, but the sources had the following types:\n{}{}{}\n",
                indent,
                indent,
                INDENT,
                types
            ),
            Self::SinkTypeMismatch(types) => format!(
                "{}Sinks for service must be identical, but the sinks had the following types:\n{}{}{}\n",
                indent,
                indent,
                INDENT,
                types
            ),
            Self::InvalidState(error) => format!(
                "{}State is invalid:\n{}",
                indent,
                error.readable(indents + 1)
            ),
            Self::InvalidTransformsSteps(error) => format!(
                "{}Transforms block is invalid:\n{}",
                indent,
                error.readable(indents + 1)
            ),
            Self::InvalidPostTransforms(error) => error.readable(indents)
        }
    }
}

impl Default for Operations {
    fn default() -> Self {
        Operations {
            name: "".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Default::default(),
            post_transforms: None,
            states: vec![],
        }
    }
}

impl Operations {
    pub fn add_operator(
        &mut self,
        operator_type: OperatorType,
        operator_placement: OperatorPlacement,
        step_invocation: StepInvocation,
    ) -> Result<()> {
        if operator_placement.window {
            match self.post_transforms {
                Some(PostTransforms::AssignTimestamp(ref mut window)) => window.add_operator(
                    operator_placement.transforms_index,
                    operator_placement.partition,
                    operator_type,
                    step_invocation,
                ),
                _ => Err(anyhow!(
                    "Cannot add operator. Window was specified but service does not have a window"
                )),
            }
        } else if operator_placement.partition {
            match self.post_transforms {
                Some(PostTransforms::Partition(ref mut partition)) => {
                    partition.add_operator(
                        operator_placement.transforms_index,
                        operator_type,
                        step_invocation,
                    )
                }
                Some(PostTransforms::AssignTimestamp(_)) => {
                    Err(
                        anyhow!("Cannot add operator. Service does not have top level partition. To delete operator from window partition, please specify window")
                    )
                }
                None => {
                    Err(anyhow!("Cannot add operator. Parition was specified but service does not have a partition"))
                }
            }
        } else {
            self.transforms.insert_operator(
                operator_placement.transforms_index,
                operator_type,
                step_invocation,
            )
        }
    }

    pub fn delete_operator(&mut self, operator_placement: OperatorPlacement) -> Result<()> {
        if operator_placement.window {
            match self.post_transforms {
                Some(PostTransforms::AssignTimestamp(ref mut window)) => {
                    window.delete_operator(operator_placement.transforms_index, operator_placement.partition)
                },
                _ => {
                    Err(anyhow!("Cannot delete operator. Window was specified but service does not have a window"))
                }
            }
        } else if operator_placement.partition {
            match self.post_transforms {
                Some(PostTransforms::Partition(ref mut partition)) => {
                    partition.delete_operator(operator_placement.transforms_index)
                },
                Some(PostTransforms::AssignTimestamp(_)) => {
                    Err(
                        anyhow!("Cannot delete operator. Service does not have top level partition. To delete operator from window partition, please specify window")
                    )
                },
                None => {
                    Err(anyhow!("Cannot delete operator. Parition was specified but service does not have a partition"))
                }
            }
        } else {
            match operator_placement.transforms_index {
                Some(index) => self.transforms.delete_operator(index),
                None => Err(anyhow!(
                    "Transforms index required to delete operator from transforms"
                )),
            }
        }
    }

    pub fn import_operator_configs(
        &mut self,
        imports: &[PackageImport],
        packages: &[PackageDefinition],
    ) -> Result<()> {
        let mut service_states: BTreeMap<_, _> = self
            .states
            .iter()
            .map(|s| (s.name().to_owned(), s.clone()))
            .collect();
        for source in &mut self.sources {
            if let IoType::Topic = source.type_ {
                for step in &mut source.steps {
                    if step.is_imported(imports) {
                        *step = imported_operator_config(step, imports, packages)?;
                    }
                }
            }
        }

        for sink in &mut self.sinks {
            if let IoType::Topic = sink.type_ {
                for step in &mut sink.steps {
                    if step.is_imported(imports) {
                        *step = imported_operator_config(step, imports, packages)?;

                        inject_states(&mut service_states, &step.inner().states)?;
                    }
                }
            }
        }

        for step in &mut self.transforms.steps {
            if step.is_imported(imports) {
                *step = imported_operator_config(step, imports, packages)?;
                inject_states(&mut service_states, &step.inner().states)?;
            }
        }

        if let Some(ref mut post_transforms) = self.post_transforms {
            match post_transforms {
                PostTransforms::AssignTimestamp(window) => {
                    window.import_operator_configs(imports, packages, &mut service_states)?;
                }
                PostTransforms::Partition(partition) => {
                    partition.import_operator_configs(imports, packages, &mut service_states)?;
                }
            }
        }

        self.states = service_states.into_values().collect();

        Ok(())
    }

    #[cfg(feature = "parser")]
    /// parse each inline operator and update the operator with the correct operator type
    pub fn update_inline_operators(&mut self) -> Result<()> {
        for source in &mut self.sources {
            if let IoType::Topic = source.type_ {
                for step in &mut source.steps {
                    step.update_signature_from_code()?;
                }
            }
        }

        for sink in &mut self.sinks {
            if let IoType::Topic = sink.type_ {
                for step in &mut sink.steps {
                    step.update_signature_from_code()?;
                }
            }
        }

        for step in &mut self.transforms.steps {
            step.update_signature_from_code()?;
        }

        if let Some(ref mut post_transforms) = self.post_transforms {
            post_transforms.update_inline_operators()?;
        }
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        types: &SdfTypesMap,
        topics: &[(String, Topic)],
        schedules: Option<&[ScheduleConfig]>,
    ) -> Result<(), ServiceValidationFailure> {
        let mut failure = ServiceValidationFailure {
            name: self.name.clone(),
            errors: vec![],
        };

        if self.name.is_empty() {
            failure.errors.push(ServiceValidationError::NameEmpty);
        }

        let service_input_type = match self.service_input_type(topics) {
            Ok(ty) => ty,
            Err(e) => {
                failure.errors.push(e);
                return Err(failure);
            }
        };

        if let Err(e) = self.validate_sources(types, topics, schedules) {
            failure.errors = [failure.errors, e].concat();
        }

        if let Err(e) = self.validate_states() {
            for error in e.errors {
                failure
                    .errors
                    .push(ServiceValidationError::InvalidState(error));
            }
        }

        if let Err(transforms_validation_errors) = validate_transforms_steps(
            &self.transforms.steps,
            types,
            service_input_type.clone(),
            "sources".to_string(),
        ) {
            failure
                .errors
                .push(ServiceValidationError::InvalidTransformsSteps(
                    transforms_validation_errors,
                ));
        }

        let transforms_output_type = match self.transforms.output_type(service_input_type) {
            Ok(ty) => ty,
            Err(e) => {
                failure
                    .errors
                    .push(ServiceValidationError::InvalidTransformsSteps(
                        ValidationFailure { errors: vec![e] },
                    ));

                return Err(failure);
            }
        };

        if let Some(post_transforms) = &self.post_transforms {
            if let Err(post_transforms_error) =
                post_transforms.validate(types, &transforms_output_type)
            {
                failure
                    .errors
                    .push(ServiceValidationError::InvalidPostTransforms(
                        post_transforms_error,
                    ));
            }
        }

        let service_output_type = match self.post_transforms_output_type(transforms_output_type) {
            Ok(ty) => ty,
            Err(_e) => {
                // should be unreachable since we already validated the transforms
                return Err(failure);
            }
        };

        if let Err(e) = self.validate_sinks(types, topics, service_output_type) {
            failure.errors = [failure.errors, e].concat();
        }

        if failure.errors.is_empty() {
            Ok(())
        } else {
            Err(failure)
        }
    }

    fn validate_states(&self) -> Result<(), ValidationFailure> {
        validate_all(&self.states)
    }

    fn validate_sources(
        &self,
        types: &SdfTypesMap,
        topics: &[(String, Topic)],
        schedules: Option<&[ScheduleConfig]>,
    ) -> Result<(), Vec<ServiceValidationError>> {
        let mut errors = vec![];
        let mut source_types = vec![];

        for source in &self.sources {
            if let Err(source_error) = source.validate_source(types, topics, schedules) {
                errors.push(ServiceValidationError::InvalidSource(source_error));
            }

            if let Ok(source_type) = source.source_type(topics) {
                source_types.push((source.id.clone(), source_type));
            }
        }

        // test the source types match
        if !types_are_identical(&source_types) {
            errors.push(ServiceValidationError::SourceTypeMismatch(
                source_types
                    .iter()
                    .map(|(source_name, type_)| {
                        if let Some(key) = &type_.key {
                            format!(
                                "{}: {}(key) - {}(value)",
                                source_name, key.name, type_.value.name
                            )
                        } else {
                            format!("{}: {}(value)", source_name, type_.value.name)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn validate_sinks(
        &self,
        types: &SdfTypesMap,
        topics: &[(String, Topic)],
        service_output_type: KVSchemaType,
    ) -> Result<(), Vec<ServiceValidationError>> {
        let mut errors = vec![];
        let mut sink_types = vec![];

        for sink in &self.sinks {
            if let Err(sink_error) = sink.validate_sink(types, topics, &service_output_type) {
                errors.push(ServiceValidationError::InvalidSink(sink_error));
            }

            if let Ok(Some(source_type)) = sink.sink_type(topics) {
                sink_types.push((sink.id.clone(), source_type));
            }
        }

        if !types_are_identical(&sink_types) {
            errors.push(ServiceValidationError::SinkTypeMismatch(
                sink_types
                    .iter()
                    .map(|(sink_name, type_)| {
                        if let Some(key) = &type_.key {
                            format!(
                                "{}: {}(key) - {}(value)",
                                sink_name, key.name, type_.value.name
                            )
                        } else {
                            format!("{}: {}(value)", sink_name, type_.value.name)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn service_input_type(
        &self,
        topics: &[(String, Topic)],
    ) -> Result<KVSchemaType, ServiceValidationError> {
        match self.sources.first() {
            Some(source) => match source.source_type(topics) {
                Ok(ty) => Ok(ty),
                _ => Err(ServiceValidationError::MissingSourceTopic(
                    source.id.clone(),
                )),
            },
            None => Err(ServiceValidationError::NoSources),
        }
    }

    fn post_transforms_output_type(
        &self,
        transforms_output_type: KVSchemaType,
    ) -> Result<KVSchemaType, ValidationError> {
        if let Some(post_transforms) = &self.post_transforms {
            post_transforms.output_type(transforms_output_type)
        } else {
            Ok(transforms_output_type)
        }
    }

    pub fn operators(&self) -> Vec<(StepInvocation, OperatorType)> {
        let ops = self
            .transforms
            .steps
            .iter()
            .map(|op| (op.inner().to_owned(), op.clone().into()))
            .chain(
                self.post_transforms
                    .iter()
                    .flat_map(|post_transforms| post_transforms.operators()),
            )
            .collect();

        ops
    }
}

fn types_are_identical(types: &[(String, KVSchemaType)]) -> bool {
    let Some((_, mut sample_ty)) = types.first().cloned() else {
        return true;
    };

    for (_, current_ty) in types.iter().skip(1) {
        if current_ty.value != sample_ty.value {
            return false;
        }

        match (&current_ty.key, &sample_ty.key) {
            (Some(k), Some(fk)) => {
                if k != fk {
                    return false;
                }
            }
            (Some(k), None) => sample_ty.key = Some(k.clone()),
            _ => (),
        }
    }
    true
}

#[cfg(test)]
mod test {

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use crate::{
        metadata::dataflow::{
            io_ref::{IoRefValidationError, IoRefValidationFailure},
            operations::ServiceValidationError,
        },
        util::{
            config_error::ConfigError, operator_placement::OperatorPlacement,
            sdf_types_map::SdfTypesMap, validation_error::ValidationError,
            validation_failure::ValidationFailure,
        },
        wit::{
            dataflow::{
                IoRef, IoType, Operations, PackageDefinition, PackageImport, PostTransforms, Topic,
            },
            io::{SchemaSerDe, TopicSchema, TypeRef},
            metadata::{
                NamedParameter, OutputType, Parameter, ParameterKind, SdfKeyedState,
                SdfKeyedStateValue,
            },
            operator::{
                PartitionOperator, StepInvocation, StepState, TransformOperator, Transforms,
                TumblingWindow, Window, WindowProperties, WindowKind, WatermarkConfig,
            },
            package_interface::{FunctionImport, Header, OperatorType, StateTyped},
            states::{State, StateRef},
        },
    };

    fn packages() -> Vec<PackageDefinition> {
        vec![PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_owned(),
            meta: Header {
                name: "my-pkg".to_string(),
                namespace: "my-ns".to_string(),
                version: "0.1.0".to_string(),
            },
            functions: vec![my_fn()],
            imports: vec![],
            types: vec![],
            states: vec![StateTyped {
                name: "map-state".to_string(),
                type_: SdfKeyedState {
                    key: TypeRef {
                        name: "string".to_string(),
                    },
                    value: SdfKeyedStateValue::U32,
                },
            }],
            dev: None,
        }]
    }

    fn my_fn() -> (StepInvocation, OperatorType) {
        (
            StepInvocation {
                uses: "map-fn".to_string(),
                inputs: vec![NamedParameter {
                    name: "map-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "u8".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                states: vec![StepState::Resolved(StateTyped {
                    name: "map-state".to_string(),
                    type_: SdfKeyedState {
                        key: TypeRef {
                            name: "string".to_string(),
                        },
                        value: SdfKeyedStateValue::U32,
                    },
                })],
                ..Default::default()
            },
            OperatorType::Map,
        )
    }

    fn imports() -> Vec<PackageImport> {
        vec![PackageImport {
            metadata: Header {
                name: "my-pkg".to_string(),
                namespace: "my-ns".to_string(),
                version: "0.1.0".to_string(),
            },
            functions: vec![FunctionImport {
                name: "map-fn".to_string(),
                alias: None,
            }],
            path: Some("path/to/my-pkg".to_string()),
            types: vec![],
            states: vec![],
        }]
    }

    fn operations() -> Operations {
        Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-source".to_string(),
                steps: vec![TransformOperator::Map(StepInvocation {
                    uses: "map-fn".to_string(),
                    ..Default::default()
                })],
            }],
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-source".to_string(),
                steps: vec![TransformOperator::Map(StepInvocation {
                    uses: "map-fn".to_string(),
                    ..Default::default()
                })],
            }],
            transforms: Transforms {
                steps: vec![
                    TransformOperator::Map(StepInvocation {
                        uses: "map-fn".to_string(),
                        ..Default::default()
                    }),
                    TransformOperator::Map(StepInvocation {
                        uses: "map-fn".to_string(),
                        ..Default::default()
                    }),
                ],
            },
            post_transforms: None,
            states: vec![],
        }
    }

    fn topics() -> Vec<(String, Topic)> {
        vec![
            (
                "my-topic".to_string(),
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
            ),
            (
                "my-other-topic".to_string(),
                Topic {
                    name: "my-other-topic".to_string(),
                    schema: TopicSchema {
                        key: None,
                        value: SchemaSerDe {
                            converter: None,
                            type_: TypeRef {
                                name: "u16".to_string(),
                            },
                        },
                    },
                    consumer: None,
                    producer: None,
                    profile: None,
                },
            ),
            (
                "my-third-topic".to_string(),
                Topic {
                    name: "my-other-topic".to_string(),
                    schema: TopicSchema {
                        key: None,
                        value: SchemaSerDe {
                            converter: None,
                            type_: TypeRef {
                                name: "u16".to_string(),
                            },
                        },
                    },
                    consumer: None,
                    producer: None,
                    profile: None,
                },
            ),
            (
                "my-topic-with-key".to_string(),
                Topic {
                    name: "my-topic-with-key".to_string(),
                    schema: TopicSchema {
                        key: Some(SchemaSerDe {
                            converter: None,
                            type_: TypeRef {
                                name: "string".to_string(),
                            },
                        }),
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
            ),
            (
                "my-topic-with-another-key".to_string(),
                Topic {
                    name: "my-topic-with-another-key".to_string(),
                    schema: TopicSchema {
                        key: Some(SchemaSerDe {
                            converter: None,
                            type_: TypeRef {
                                name: "bytes".to_string(),
                            },
                        }),
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
            ),
        ]
    }

    fn function() -> StepInvocation {
        StepInvocation {
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
        }
    }

    fn service_with_window() -> Operations {
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

    fn service_with_partition() -> Operations {
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

        let post_transforms = Some(PostTransforms::Partition(PartitionOperator {
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
    fn test_import_operator_configs_merges_operator_signatures() {
        let mut operations = operations();

        let source_steps = &operations.sources.first().unwrap().steps;
        assert!(source_steps.first().unwrap().inner().inputs.is_empty());
        assert!(source_steps.first().unwrap().inner().output.is_none());
        assert!(source_steps.first().unwrap().inner().states.is_empty());

        let sink_steps = &operations.sinks.first().unwrap().steps;
        assert!(sink_steps.first().unwrap().inner().inputs.is_empty());
        assert!(sink_steps.first().unwrap().inner().output.is_none());
        assert!(sink_steps.first().unwrap().inner().states.is_empty());

        let steps = &operations.transforms.steps;
        assert!(steps.first().unwrap().inner().inputs.is_empty());
        assert!(steps.first().unwrap().inner().output.is_none());
        assert!(steps.first().unwrap().inner().states.is_empty());

        assert!(operations.states.is_empty());

        operations
            .import_operator_configs(&imports(), &packages())
            .unwrap();

        let source_steps = &operations.sources.first().unwrap().steps;
        assert_eq!(source_steps.first().unwrap().inner().inputs.len(), 1);
        assert!(source_steps.first().unwrap().inner().output.is_some());
        assert_eq!(source_steps.first().unwrap().inner().states.len(), 1);

        let sink_steps = &operations.sinks.first().unwrap().steps;
        assert_eq!(sink_steps.first().unwrap().inner().inputs.len(), 1);
        assert!(sink_steps.first().unwrap().inner().output.is_some());
        assert_eq!(sink_steps.first().unwrap().inner().states.len(), 1);

        let steps = &operations.transforms.steps;
        assert_eq!(steps.first().unwrap().inner().inputs.len(), 1);
        assert!(steps.first().unwrap().inner().output.is_some());
        assert_eq!(steps.first().unwrap().inner().states.len(), 1);

        assert_eq!(operations.states.len(), 1);
    }

    #[test]
    fn test_validate_rejects_service_without_name() {
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &[], None)
            .expect_err("should error for missing service name");

        assert!(res.errors.contains(&ServiceValidationError::NameEmpty));

        assert!(res.readable(0).contains(
            r#"Service `` is invalid:
    Service name cannot be empty
"#
        ));
    }

    #[test]
    fn test_validate_validates_states() {
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![State::Reference(StateRef {
                name: "my-state".to_string(),
                ref_service: "".to_string(),
            })],
        };

        let res = service
            .validate(&types, &topics(), None)
            .expect_err("should error for invalid ref state");

        assert!(res.errors.contains(&ServiceValidationError::InvalidState(ValidationError::new(
            "service name missing for state reference. state reference must be of the form <service>.<state>"
        ))));
        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    State is invalid:
        service name missing for state reference. state reference must be of the form <service>.<state>
"#
        );
    }

    #[test]
    fn test_validate_rejects_service_without_sources() {
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &[], None)
            .expect_err("should error for missing sources");

        assert!(res.errors.contains(&ServiceValidationError::NoSources));
        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Service must have at least one source
"#
        );
    }

    #[test]
    fn test_validate_sources_validates_each_source() {
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-source-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &[], None)
            .expect_err("should error for missing sources");

        assert!(res
            .errors
            .contains(&ServiceValidationError::MissingSourceTopic(
                "my-source-topic".to_string()
            )));
        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Source topic `my-source-topic` not found
"#
        );
    }

    #[test]
    fn test_validate_rejects_sources_when_key_types_differ() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic-with-key".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic-with-another-key".to_string(),
                    steps: vec![],
                },
            ],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should error for source type mismatch");

        assert!(res.errors.contains(&ServiceValidationError::SourceTypeMismatch(
            "my-topic: u8(value), my-topic-with-key: string(key) - u8(value), my-topic-with-another-key: bytes(key) - u8(value)".to_string()
        )));
        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sources for service must be identical, but the sources had the following types:
        my-topic: u8(value), my-topic-with-key: string(key) - u8(value), my-topic-with-another-key: bytes(key) - u8(value)
"#
        );
    }

    #[test]
    fn test_validate_rejects_sources_when_type_values_differ() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-other-topic".to_string(),
                    steps: vec![],
                },
            ],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should error for source type mismatch");

        assert!(res
            .errors
            .contains(&ServiceValidationError::SourceTypeMismatch(
                "my-topic: u8(value), my-other-topic: u16(value)".to_string()
            )));

        // same case but with steps
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-other-topic".to_string(),
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u16".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "string".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })],
                },
            ],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should error for source type mismatch");

        assert!(res
            .errors
            .contains(&ServiceValidationError::SourceTypeMismatch(
                "my-topic: u8(value), my-other-topic: string(value)".to_string()
            )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sources for service must be identical, but the sources had the following types:
        my-topic: u8(value), my-other-topic: string(value)
"#
        );
    }

    #[test]
    fn test_validate_accepts_valid_sources() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-other-topic".to_string(),
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u16".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "u8".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })],
                },
            ],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        service
            .validate(&types, &topics, None)
            .expect("should validate")
    }

    #[test]
    fn test_validate_sinks_validates_each_sink() {
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-sink-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics(), None)
            .expect_err("should error for missing sink topic");

        assert!(res.errors.contains(&ServiceValidationError::InvalidSink(
            IoRefValidationFailure {
                name: "my-sink-topic".to_string(),
                errors: vec![IoRefValidationError::InvalidRef(
                    "my-sink-topic".to_string()
                )]
            }
        )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sink `my-sink-topic` is invalid:
        Referenced topic `my-sink-topic` not found
"#
        );
    }

    #[test]
    fn test_validate_rejects_sinks_when_key_types_differ() {
        let topics = topics();
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic-with-key".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic-with-another-key".to_string(),
                    steps: vec![],
                },
            ],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should error for sink type mismatch");

        assert!(res.errors.contains(&ServiceValidationError::SinkTypeMismatch(
            "my-topic-with-key: string(key) - u8(value), my-topic-with-another-key: bytes(key) - u8(value)".to_string()
        )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sinks for service must be identical, but the sinks had the following types:
        my-topic-with-key: string(key) - u8(value), my-topic-with-another-key: bytes(key) - u8(value)
"#
        );
    }

    #[test]
    fn test_validate_rejects_sinks_when_value_types_differ() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-other-topic".to_string(),
                    steps: vec![],
                },
            ],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should error for sink type mismatch");

        assert!(res
            .errors
            .contains(&ServiceValidationError::SinkTypeMismatch(
                "my-topic: u8(value), my-other-topic: u16(value)".to_string()
            )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sink `my-other-topic` is invalid:
        Transforms block is invalid:
            service output type `u8` does not match sink input type `u16`
    Sinks for service must be identical, but the sinks had the following types:
        my-topic: u8(value), my-other-topic: u16(value)
"#
        );
    }

    #[test]
    fn test_validate_accepts_valid_sinks() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            sinks: vec![
                IoRef {
                    type_: IoType::Topic,
                    id: "my-topic".to_string(),
                    steps: vec![],
                },
                IoRef {
                    type_: IoType::Topic,
                    id: "my-other-topic".to_string(),
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "u16".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })],
                },
            ],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        service
            .validate(&types, &topics, None)
            .expect("should validate")
    }

    #[test]
    fn test_validate_validates_transforms() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms {
                steps: vec![TransformOperator::Map(StepInvocation {
                    uses: "my-function".to_string(),
                    inputs: vec![NamedParameter {
                        name: "input".to_string(),
                        type_: TypeRef {
                            name: "u8".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    }],
                    output: Some(Parameter {
                        type_: TypeRef {
                            name: "foobar".to_string(),
                        }
                        .into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })],
            },
            sinks: vec![],
            post_transforms: None,
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should fail for output type not in scope");

        assert!(res.errors.contains(&ServiceValidationError::InvalidTransformsSteps(
            ValidationFailure {
                errors: vec![ValidationError::new("function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types")]
            }
        )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Transforms block is invalid:
        function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types
"#
        );
    }

    #[test]
    fn test_validate_transforms_with_different_keys() {
        let topics = topics();
        let types = SdfTypesMap::default();
        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic-with-key".to_string(),
                steps: vec![],
            }],
            sinks: vec![],
            transforms: Transforms {
                steps: vec![
                    TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "string".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    TransformOperator::Filter(StepInvocation {
                        uses: "my-other-function".to_string(),
                        inputs: vec![
                            NamedParameter {
                                name: "key".to_string(),
                                type_: TypeRef {
                                    name: "bytes".to_string(),
                                },
                                optional: false,
                                kind: ParameterKind::Key,
                            },
                            NamedParameter {
                                name: "input".to_string(),
                                type_: TypeRef {
                                    name: "string".to_string(),
                                },
                                optional: false,
                                kind: ParameterKind::Value,
                            },
                        ],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "bool".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                ],
            },

            states: vec![],
            post_transforms: None,
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should fail for steps with different keys");

        assert!(res
            .errors
            .contains(&ServiceValidationError::InvalidTransformsSteps(
                ValidationFailure {
                    errors: vec![ValidationError::new(
                    "in `my-other-function`, key type does not match expected key type. bytes != string"
                )],
                }
            )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Transforms block is invalid:
        in `my-other-function`, key type does not match expected key type. bytes != string
"#
        );
    }

    #[test]
    fn test_validate_validates_partition() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms { steps: vec![] },
            sinks: vec![],
            post_transforms: Some(PostTransforms::Partition(PartitionOperator {
                assign_key: StepInvocation {
                    uses: "my-assign-key".to_string(),
                    inputs: vec![NamedParameter {
                        name: "input".to_string(),
                        type_: TypeRef {
                            name: "u8".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    }],
                    output: Some(Parameter {
                        type_: TypeRef {
                            name: "string".to_string(),
                        }
                        .into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                transforms: Transforms {
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "foobar".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })],
                },
                update_state: None,
            })),
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should fail for output type not in scope");

        assert!(res.errors.contains(&ServiceValidationError::InvalidPostTransforms(
            ValidationFailure {
                errors: vec![ValidationError::new("Partition transforms block is invalid: function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types")]
            }
        )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Partition transforms block is invalid: function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types
"#
        );
    }

    #[test]
    fn test_validate_validates_window() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms { steps: vec![] },
            sinks: vec![],
            post_transforms: Some(PostTransforms::AssignTimestamp(Window {
                transforms: Transforms {
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-function".to_string(),
                        inputs: vec![NamedParameter {
                            name: "input".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "foobar".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })],
                },
                ..Default::default()
            })),
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should fail for output type not in scope");
        println!("{:#?}", res);

        assert!(res.errors.iter().any(|e|{
            if let ServiceValidationError::InvalidPostTransforms(failure) = e {
                if failure.errors.contains(
                    &ValidationError::new("Window transforms block is invalid: function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types")
                ) {
                    return true;
                }
            }

            false
        }));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Window assign-timestamp type function `` should have exactly 2 input type, found 0
    Window assign-timestamp type function `` requires an output type
    Window transforms block is invalid: function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types
"#
        );
    }

    fn map_fn(name: String, input_type: String, output_type: String) -> TransformOperator {
        TransformOperator::Map(StepInvocation {
            uses: name,
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef { name: input_type },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef { name: output_type }.into(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn assign_timestamp_fn(input_type: String) -> StepInvocation {
        StepInvocation {
            uses: "assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef { name: input_type },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "timestamp".to_string(),
                    type_: TypeRef {
                        name: "i64".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "i64".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn assign_key_fn(input_type: String) -> StepInvocation {
        StepInvocation {
            uses: "assign-timestamp".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef { name: input_type },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn window_aggregate_fn(output_type: String) -> StepInvocation {
        StepInvocation {
            uses: "window-aggregate".to_string(),
            inputs: vec![],
            output: Some(Parameter {
                type_: TypeRef { name: output_type }.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_types_validate_throughout() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![map_fn("1".to_string(), "u8".to_string(), "u32".to_string())],
            }],
            transforms: Transforms {
                steps: vec![map_fn(
                    "2".to_string(),
                    "u32".to_string(),
                    "u64".to_string(),
                )],
            },
            post_transforms: Some(PostTransforms::AssignTimestamp(Window {
                assign_timestamp: assign_timestamp_fn("u64".to_string()),
                transforms: Transforms {
                    steps: vec![map_fn("3".to_string(), "u64".to_string(), "i8".to_string())],
                },
                partition: Some(PartitionOperator {
                    assign_key: assign_key_fn("i8".to_string()),
                    transforms: Transforms {
                        steps: vec![map_fn("4".to_string(), "i8".to_string(), "i16".to_string())],
                    },
                    update_state: None,
                }),
                flush: Some(window_aggregate_fn("u16".to_string())),
                ..Default::default()
            })),
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-other-topic".to_string(),
                steps: vec![],
            }],
            states: vec![],
        };

        service
            .validate(&types, &topics, None)
            .expect("should validate");
    }

    #[test]
    fn test_operators() {
        let operations = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![map_fn(
                    "my-step".to_string(),
                    "u8".to_string(),
                    "u32".to_string(),
                )],
            }],
            transforms: Transforms {
                steps: vec![map_fn(
                    "my-map".to_string(),
                    "u32".to_string(),
                    "u64".to_string(),
                )],
            },
            post_transforms: None,
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-other-topic".to_string(),
                steps: vec![],
            }],
            states: vec![],
        };

        let ops = operations.operators();

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0.uses, "my-map");
    }

    #[test]
    fn test_validate_types_when_there_are_no_transforms_steps() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-other-topic".to_string(),
                steps: vec![],
            }],
            states: vec![],
        };

        let res = service
            .validate(&types, &topics, None)
            .expect_err("should validate");

        assert!(res.errors.contains(&ServiceValidationError::InvalidSink(
            IoRefValidationFailure {
                name: "my-other-topic".to_string(),
                errors: vec![IoRefValidationError::InvalidTransformsBlock(vec![
                    ValidationError::new(
                        "service output type `u8` does not match sink input type `u16`"
                    )
                ])]
            }
        )));

        assert_eq!(
            res.readable(0),
            r#"Service `my-service` is invalid:
    Sink `my-other-topic` is invalid:
        Transforms block is invalid:
            service output type `u8` does not match sink input type `u16`
"#
        );
    }

    #[test]
    fn test_validate_topic_without_key_with_function_that_requires_key() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let service = Operations {
            name: "my-service".to_string(),
            sources: vec![IoRef {
                type_: IoType::Topic,
                id: "my-topic".to_string(),
                steps: vec![],
            }],
            transforms: Transforms {
                steps: vec![TransformOperator::Map(StepInvocation {
                    uses: "my-function".to_string(),
                    inputs: vec![
                        NamedParameter {
                            name: "k".to_string(),
                            type_: TypeRef {
                                name: "string".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Key,
                        },
                        NamedParameter {
                            name: "value".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        },
                    ],
                    output: Some(Parameter {
                        type_: TypeRef {
                            name: "u8".to_string(),
                        }
                        .into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })],
            },
            post_transforms: None,
            sinks: vec![IoRef {
                type_: IoType::Topic,
                id: "my-other-topic".to_string(),
                steps: vec![],
            }],
            states: vec![],
        };

        let resp = service
            .validate(&types, &topics, None)
            .expect_err("should validate");

        assert!(
            resp.errors.contains(&ServiceValidationError::InvalidTransformsSteps(
                ValidationFailure {
                    errors: vec![ValidationError::new(
                        "my-function function requires a key, but none was found. Make sure that you define the right key in the topic configuration"
                    )]
                }
            ))
        );
    }

    #[test]
    fn test_add_operator() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(2),
            ..Default::default()
        };

        let function = function();

        service
            .add_operator(OperatorType::Map, operator_placement, function.clone())
            .expect("Failed to add imported operator");

        let result_operator = service.transforms.steps[2].clone();

        assert_eq!(result_operator, TransformOperator::Map(function));
    }

    #[test]
    fn test_add_partition_transforms_operator() {
        let mut service = service_with_partition();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            transforms_index: Some(1),
            partition: true,
            window: false,
        };

        let function = function();

        service
            .add_operator(OperatorType::Map, operator_placement, function.clone())
            .expect("Failed to add imported operator");

        let result_operator = match service.post_transforms {
            Some(PostTransforms::Partition(partition)) => partition.transforms.steps[1].clone(),
            _ => panic!("expected partition"),
        };

        assert_eq!(result_operator, TransformOperator::Map(function));
    }

    #[test]
    fn test_add_partition_operator_when_window_incorrectly_specified() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: true,
            transforms_index: Some(0),
        };

        let function = function();

        let res = service.add_operator(OperatorType::Map, operator_placement, function);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot add operator. Service does not have top level partition. To delete operator from window partition, please specify window"
        )
    }

    #[test]
    fn test_add_partition_operator_with_no_partition() {
        let mut service = Operations {
            name: "listing-to-prospect-op".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: true,
            transforms_index: Some(0),
        };

        let function = function();

        let res = service.add_operator(OperatorType::Map, operator_placement, function);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot add operator. Parition was specified but service does not have a partition"
        )
    }

    #[test]
    fn test_add_window_operator() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let function = function();
        let res = service.add_operator(OperatorType::Map, operator_placement, function);

        let post_transforms = service.post_transforms.as_ref().unwrap();
        let window = match post_transforms {
            PostTransforms::AssignTimestamp(w) => w,
            _ => panic!("expected window"),
        };

        assert!(res.is_ok());
        assert_eq!(window.transforms.steps.len(), 2);
    }

    #[test]
    fn test_add_window_operator_when_partition_but_no_window() {
        let mut service = service_with_partition();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let function = function();
        let res = service.add_operator(OperatorType::Map, operator_placement, function);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot add operator. Window was specified but service does not have a window"
        )
    }

    #[test]
    fn test_add_window_operator_when_no_window() {
        let mut service = Operations {
            name: "listing-to-prospect-op".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let function = function();
        let res = service.add_operator(OperatorType::Map, operator_placement, function);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot add operator. Window was specified but service does not have a window"
        )
    }

    // fn test_add_assign_key() {
    //}

    // fn test_add_assign_key_without_partition() {
    // should Fail
    // }

    // fn test_add_assign_timestamp() {
    //}

    // fn test_add_assign_timestamp_without_window() {
    // should Fail
    //}

    // fn test_add_update_state() {
    //}

    // fn test_add_update_state_without_state() {
    // should Fail
    //}

    #[test]
    fn test_delete_operator_deletes_op_from_transforms() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: false,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        assert!(res.is_ok());
        assert_eq!(service.transforms.steps.len(), 1);
    }

    #[test]
    fn test_delete_operator_deletes_op_from_partition() {
        let mut service = service_with_partition();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: true,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        let post_transforms = service.post_transforms.as_ref().unwrap();
        let partition = match post_transforms {
            PostTransforms::Partition(p) => p,
            _ => panic!("expected partition"),
        };

        assert!(res.is_ok());
        assert_eq!(partition.transforms.steps.len(), 0);

        // doesn't incorrectly delete something from transforms
        assert_eq!(service.transforms.steps.len(), 2)
    }

    #[test]
    fn test_delete_partition_operator_when_window_incorrectly_specified() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: true,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot delete operator. Service does not have top level partition. To delete operator from window partition, please specify window"
        )
    }

    #[test]
    fn test_delete_partition_operator_with_no_partition() {
        let mut service = Operations {
            name: "listing-to-prospect-op".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: false,
            partition: true,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot delete operator. Parition was specified but service does not have a partition"
        )
    }

    #[test]
    fn test_delete_operator_deletes_op_from_window() {
        let mut service = service_with_window();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        let post_transforms = service.post_transforms.as_ref().unwrap();
        let window = match post_transforms {
            PostTransforms::AssignTimestamp(w) => w,
            _ => panic!("expected window"),
        };

        assert!(res.is_ok());
        assert_eq!(window.transforms.steps.len(), 0);

        // doesn't incorrectly delete something from transforms
        assert_eq!(service.transforms.steps.len(), 2)
    }

    #[test]
    fn test_delete_window_operator_when_partition_but_no_window() {
        let mut service = service_with_partition();

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot delete operator. Window was specified but service does not have a window"
        )
    }

    #[test]
    fn test_delete_window_operator_when_no_window() {
        let mut service = Operations {
            name: "listing-to-prospect-op".to_string(),
            sources: vec![],
            sinks: vec![],
            transforms: Transforms { steps: vec![] },
            post_transforms: None,
            states: vec![],
        };

        let operator_placement = OperatorPlacement {
            service_id: "listing-to-prospect-op".to_string(),
            window: true,
            partition: false,
            transforms_index: Some(0),
        };

        let res = service.delete_operator(operator_placement);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot delete operator. Window was specified but service does not have a window"
        )
    }
}
