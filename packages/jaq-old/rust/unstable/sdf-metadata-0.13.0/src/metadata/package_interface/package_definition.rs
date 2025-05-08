use std::{collections::BTreeMap, fmt::Display};

use anyhow::Result;
use wit_encoder::{Package, Use};

use sdf_common::{render::wit_name_case, version::ApiVersion};

use crate::{
    importer::resolver::DependencyResolver,
    metadata::metadata::header::HeaderValidationError,
    util::{
        merge::merge_types_and_states,
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
        validate::{validate_all, MetadataTypeValidationFailure},
        validation_failure::ValidationFailure,
    },
    wit::{
        metadata::{MetadataType, SdfType, SdfTypeOrigin},
        operator::OperatorType,
        package_interface::{PackageDefinition, StepInvocation},
    },
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PackageDefinitionValidationFailure {
    pub errors: Vec<PackageDefinitionValidationError>,
}

impl Display for PackageDefinitionValidationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Package Config failed validation\n")?;

        for error in &self.errors {
            writeln!(f, "{}", error.readable(1))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PackageDefinitionValidationError {
    Meta(Vec<HeaderValidationError>),
    Type(MetadataTypeValidationFailure),
    State(ValidationFailure),
    Function(ValidationFailure),
}

impl ConfigError for PackageDefinitionValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            PackageDefinitionValidationError::Meta(errors) => {
                let mut res = format!("{}Header is invalid:\n", indent);

                for error in errors {
                    res.push_str(&error.readable(indents + 1));
                }

                res
            }
            PackageDefinitionValidationError::Type(failure) => failure.readable(indents),
            PackageDefinitionValidationError::State(failure) => {
                format!(
                    "{}State is invalid:\n{}",
                    indent,
                    failure.readable(indents + 1)
                )
            }
            PackageDefinitionValidationError::Function(failure) => failure.readable(indents),
        }
    }
}

impl PackageDefinition {
    pub fn name(&self) -> &str {
        &self.meta.name
    }

    pub fn api_version(&self) -> Result<ApiVersion> {
        ApiVersion::from(&self.api_version)
    }

    pub fn has_custom_types(&self) -> bool {
        !self.types.is_empty() && !self.states.is_empty()
    }

    pub fn namespace(&self) -> &str {
        &self.meta.namespace
    }
    pub fn resolve_imports(&mut self, packages: Vec<PackageDefinition>, debug: bool) -> Result<()> {
        let dependency_resolver =
            DependencyResolver::build(self.imports.clone(), packages.clone(), debug)?;
        let package_configs = dependency_resolver.packages()?;

        self.merge_dependencies(&package_configs)?;

        Ok(())
    }
    pub fn merge_dependencies(&mut self, package_configs: &[PackageDefinition]) -> Result<()> {
        let mut all_types = self.types_map();
        let mut all_states = self
            .states
            .iter()
            .map(|ty| (ty.name.clone(), ty.clone()))
            .collect::<BTreeMap<_, _>>();

        merge_types_and_states(
            &mut all_types,
            &mut all_states,
            &self.imports,
            package_configs,
        )?;

        self.types = all_types
            .iter()
            .map(|(name, (ty, origin))| MetadataType {
                name: name.to_owned(),
                type_: ty.to_owned(),
                origin: origin.to_owned(),
            })
            .collect();
        self.states = all_states.into_values().collect();

        self.resolve_function_states()?;

        Ok(())
    }

    pub fn resolve_function_states(&mut self) -> Result<()> {
        for (function, _operator) in self.functions.iter_mut() {
            function.resolve_states(&self.states)?;
        }

        Ok(())
    }

    pub fn get_function(&self, name: &str) -> Option<&(StepInvocation, OperatorType)> {
        self.functions
            .iter()
            .find(|(function, _operator)| function.uses == name)
    }

    pub fn types_map(&self) -> SdfTypesMap {
        SdfTypesMap {
            map: self
                .types
                .iter()
                .map(|ty| (ty.name.clone(), (ty.type_.clone(), ty.origin)))
                .chain(self.states.iter().map(|state| {
                    (
                        state.name.clone(),
                        (
                            SdfType::KeyedState(state.type_.clone()),
                            SdfTypeOrigin::Local,
                        ),
                    )
                }))
                .collect(),
        }
    }

    pub fn validate(&self) -> Result<(), PackageDefinitionValidationFailure> {
        let mut errors: Vec<PackageDefinitionValidationError> = vec![];

        if let Err(err) = self.meta.validate() {
            errors.push(PackageDefinitionValidationError::Meta(err));
        }

        let types_map = self.types_map();

        for metadata_type in &self.types {
            if let Err(type_validation_failure) = metadata_type.validate(&types_map) {
                errors.push(PackageDefinitionValidationError::Type(
                    type_validation_failure,
                ));
            }
        }

        if let Err(err) = self.validate_states() {
            errors.push(PackageDefinitionValidationError::State(err));
        }

        if let Err(err) = self.validate_functions(&types_map) {
            errors.push(PackageDefinitionValidationError::Function(err));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(PackageDefinitionValidationFailure { errors })
        }
    }

    fn validate_states(&self) -> Result<(), ValidationFailure> {
        validate_all(&self.states)
    }

    fn validate_functions(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        for (function, operator) in &self.functions {
            match operator {
                OperatorType::AssignKey => {
                    if let Err(failures) = function.validate_assign_key(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::Map => {
                    if let Err(failures) = function.validate_map(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::FilterMap => {
                    if let Err(failures) = function.validate_filter_map(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::Filter => {
                    if let Err(failures) = function.validate_filter(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::FlatMap => {
                    if let Err(failures) = function.validate_flat_map(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::UpdateState => {
                    if let Err(failures) = function.validate_update_state(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::WindowAggregate => {
                    if let Err(failures) = function.validate_window_aggregate(types) {
                        errors.concat(&failures);
                    }
                }
                OperatorType::AssignTimestamp => {
                    if let Err(failures) = function.validate_assign_timestamp(types) {
                        errors.concat(&failures);
                    }
                }
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn types_wit_package(&self) -> Result<Package> {
        let name = wit_encoder::PackageName::new(
            self.meta.namespace.clone(),
            self.meta.name.clone(),
            None,
        );

        let mut package = Package::new(name);

        let api_version = self.api_version()?;

        let types = self.types_map();

        let imports = self
            .imports
            .iter()
            .filter_map(|import| {
                let types = import
                    .types
                    .iter()
                    .cloned()
                    .chain(import.states.iter().cloned())
                    .collect::<Vec<_>>();
                if types.is_empty() {
                    None
                } else {
                    let types_iface = format!(
                        "{}:{}/types",
                        import.metadata.namespace, import.metadata.name
                    );

                    let mut uses = Use::new(types_iface);
                    for ty in types {
                        uses.item(wit_name_case(&ty), None);
                    }
                    Some(uses)
                }
            })
            .collect::<Vec<_>>();

        let wit_interface = types.wit_interface(&api_version, imports);

        package.interface(wit_interface);
        Ok(package)
    }
}

#[cfg(test)]
mod test {

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use crate::{
        metadata::{
            metadata::{header::HeaderValidationError, sdf_type::SdfTypeValidationError},
            package_interface::package_definition::PackageDefinitionValidationError,
        },
        util::{
            validate::{MetadataTypeValidationError, MetadataTypeValidationFailure},
            validation_error::ValidationError,
            validation_failure::ValidationFailure,
        },
        wit::{
            metadata::{
                MetadataType, NamedParameter, Parameter, ParameterKind, SdfKeyedState,
                SdfKeyedStateValue, SdfObject, SdfType, SdfTypeOrigin, TypeRef,
            },
            operator::StepInvocation,
            package_interface::{
                FunctionImport, Header, OperatorType, PackageDefinition, PackageImport, StateTyped,
            },
        },
    };

    fn package() -> PackageDefinition {
        PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                namespace: "example".to_string(),
                name: "core".to_string(),
                version: "0.1.0".to_string(),
            },
            types: vec![],
            states: vec![],
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
                    types: vec!["account-balance".to_string()],
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
            functions: vec![],
            dev: None,
        }
    }

    #[test]
    fn test_validate_validates_metadata() {
        let mut pkg = package();
        pkg.meta.name = "".to_string();

        let res = pkg.validate().expect_err("should error for empty name");

        assert!(res
            .errors
            .contains(&PackageDefinitionValidationError::Meta(vec![
                HeaderValidationError::new("Name cannot be empty\n")
            ])));
        assert_eq!(
            res.to_string(),
            r#"Package Config failed validation

    Header is invalid:
        Name cannot be empty

"#,
        )
    }

    #[test]
    fn test_validate_rejects_empty_type_names() {
        let mut pkg = package();

        pkg.types = vec![MetadataType {
            name: "".to_string(),
            type_: SdfType::Object(SdfObject { fields: vec![] }),
            origin: SdfTypeOrigin::Local,
        }];

        let res = pkg
            .validate()
            .expect_err("should error for empty type name");

        assert!(res.errors.contains(&PackageDefinitionValidationError::Type(
            MetadataTypeValidationFailure {
                name: "".to_string(),
                errors: vec![MetadataTypeValidationError::EmptyName],
            }
        )));
        assert_eq!(
            res.to_string(),
            r#"Package Config failed validation

    Defined type `` is invalid:
        Name cannot be empty

"#,
        )
    }

    #[test]
    fn test_validate_validates_types() {
        let mut pkg = package();

        pkg.types.push(MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Named(TypeRef {
                name: "foobar".to_string(),
            }),
            origin: SdfTypeOrigin::Local,
        });

        let res = pkg
            .validate()
            .expect_err("should error for invalid type reference");

        assert!(res.errors.contains(&PackageDefinitionValidationError::Type(
            MetadataTypeValidationFailure {
                name: "my-type".to_string(),
                errors: vec![MetadataTypeValidationError::SdfType(
                    SdfTypeValidationError::InvalidRef("foobar".to_string())
                )],
            }
        )));
        assert_eq!(
            res.to_string(),
            r#"Package Config failed validation

    Defined type `my-type` is invalid:
        Referenced type `foobar` not found in config or imported types

"#,
        )
    }

    #[test]
    fn test_validate_validate_states() {
        let mut pkg = package();

        pkg.states.push(StateTyped {
            name: "state".to_string(),
            type_: SdfKeyedState {
                key: TypeRef {
                    name: "string".to_string(),
                },
                value: SdfKeyedStateValue::Unresolved(TypeRef {
                    name: "my-state-value".to_string(),
                }),
            },
        });

        let res = pkg
            .validate()
            .expect_err("should error for invalid state type");

        assert!(
            res.errors.contains(&PackageDefinitionValidationError::State(
                ValidationFailure {
                    errors: vec![ValidationError::new("Internal Error: typed state value should be resolved before validation. Please contact support")],
                }
            ))
        );
        assert_eq!(
            res.to_string(),
            r#"Package Config failed validation

    State is invalid:
        Internal Error: typed state value should be resolved before validation. Please contact support

"#,
        )
    }

    #[test]
    fn test_validate_validate_functions() {
        let mut pkg = package();

        pkg.functions.push((
            StepInvocation {
                uses: "my-filter".to_string(),
                inputs: vec![NamedParameter {
                    name: "first-input".to_string(),
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
            },
            OperatorType::Filter,
        ));

        let res = pkg
            .validate()
            .expect_err("should error for invalid state type");

        assert!(res.errors.contains(&PackageDefinitionValidationError::Function(
            ValidationFailure {
                errors: vec![
                    ValidationError::new("filter type function `my-filter` requires an output type of `bool`, but found `string`")
                ]
            }
        )));

        assert_eq!(
            res.to_string(),
            r#"Package Config failed validation

    filter type function `my-filter` requires an output type of `bool`, but found `string`

"#,
        )
    }

    #[test]
    fn test_validate_passes_valid_config() {
        let pkg = package();
        pkg.validate().expect("failed to validate");
    }

    #[test]
    fn test_types_wit_package() {
        let mut pkg = package();

        pkg.types = vec![MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Named(TypeRef {
                name: "foobar".to_string(),
            }),
            origin: SdfTypeOrigin::Local,
        }];

        let package = pkg
            .types_wit_package()
            .expect("failed to generate wit package");
        let expected_wit = "package example:core;

interface types {
  use example:bank-update/types.{ account-balance };
  use example:bank/types.{ bank-event };
  type bytes = list<u8>;
  type my-type = foobar;
}
";
        assert_eq!(package.to_string(), expected_wit,);
    }
}
