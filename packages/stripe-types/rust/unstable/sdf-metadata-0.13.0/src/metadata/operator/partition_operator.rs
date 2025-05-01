use std::collections::BTreeMap;

use anyhow::Result;

use crate::{
    importer::{
        function::{
            imported_assign_key_config, imported_operator_config, imported_update_state_config,
        },
        states::inject_states,
    },
    metadata::io::topic::KVSchemaType,
    util::{
        sdf_types_map::SdfTypesMap, validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::{PackageDefinition, PackageImport},
        operator::{OperatorType, PartitionOperator, StepInvocation},
        states::State,
    },
};

use super::transforms::validate_transforms_steps;

impl PartitionOperator {
    pub fn operators(&self) -> Vec<(StepInvocation, OperatorType)> {
        let mut operators = vec![(self.assign_key.clone(), OperatorType::AssignKey)];
        operators.extend(self.transforms.steps.iter().map(|step| {
            let inner = step.inner();
            (inner.clone(), step.clone().into())
        }));
        if let Some(update_state) = &self.update_state {
            operators.push((update_state.clone(), OperatorType::UpdateState));
        }
        operators
    }

    pub fn add_operator(
        &mut self,
        operator_index: Option<usize>,
        operator_type: OperatorType,
        step_invocation: StepInvocation,
    ) -> Result<()> {
        if operator_index.is_some() {
            self.transforms
                .insert_operator(operator_index, operator_type, step_invocation)
        } else {
            todo!("cannot add assign key unless it is optional")
        }
    }

    pub fn delete_operator(&mut self, operator_index: Option<usize>) -> Result<()> {
        match operator_index {
            Some(index) => self.transforms.delete_operator(index),
            None => {
                todo!("cannot delete assign key unless it is optional")
            }
        }
    }

    pub fn import_operator_configs(
        &mut self,
        imports: &[PackageImport],
        packages: &[PackageDefinition],
        service_states: &mut BTreeMap<String, State>,
    ) -> Result<()> {
        if self.assign_key.is_imported(imports) {
            self.assign_key = imported_assign_key_config(&self.assign_key, imports, packages)?;
            inject_states(service_states, &self.assign_key.states)?;
        }

        for step in &mut self.transforms.steps {
            if step.is_imported(imports) {
                *step = imported_operator_config(step, imports, packages)?;
                inject_states(service_states, &step.inner().states)?;
            }
        }

        if let Some(update_state) = &mut self.update_state {
            if update_state.is_imported(imports) {
                *update_state = imported_update_state_config(update_state, imports, packages)?;
                inject_states(service_states, &update_state.states)?;
            }
        }

        Ok(())
    }

    pub fn output_type(&self, input_type: KVSchemaType) -> Result<KVSchemaType, ValidationError> {
        self.transforms.output_type(input_type)
    }

    pub fn validate(
        &self,
        types: &SdfTypesMap,
        expected_input_type: &KVSchemaType,
        input_provider_name: &str,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(assign_key_error) =
            self.validate_assign_key(types, expected_input_type, input_provider_name)
        {
            errors.concat(&assign_key_error);
        }

        if let Err(transforms_error) = validate_transforms_steps(
            &self.transforms.steps,
            types,
            expected_input_type.to_owned(),
            input_provider_name.to_string(),
        ) {
            errors.concat_with_context("transforms block is invalid:", &transforms_error);
        }

        if let Err(update_state_error) = self.validate_update_state(types) {
            errors.concat(&update_state_error);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_assign_key(
        &self,
        types: &SdfTypesMap,
        expected_type: &KVSchemaType,
        input_provider_name: &str,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(assign_key_error) = self.assign_key.validate_assign_key(types) {
            errors.concat(&assign_key_error);
        }

        let value_type = if self.assign_key.requires_key_param() {
            let key = self
                .assign_key
                .inputs
                .first()
                .map(|input| input.type_.clone());

            if let Some(key) = key {
                if let Some(expected_key) = expected_type.key.as_ref() {
                    if key.name != expected_key.name {
                        errors.push_str(&format!(
                            "assign-key function `{}` key type should match `{}` provided by `{}` but found `{}`",
                            self.assign_key.uses,
                            expected_key.name,
                            &input_provider_name,
                            key.name
                        ));
                    }
                } else {
                    errors.push_str("assign-key type function `assign-key-fn` requires a key type");
                }

                self.assign_key.inputs.get(1)
            } else {
                errors.push_str("assign-key type function `assign-key-fn` requires an input type");
                None
            }
        } else {
            self.assign_key.inputs.first()
        };

        //assert assign key first input matches the expected input type
        if let Some(assign_key_input) = value_type {
            if assign_key_input.type_.name != expected_type.value.name {
                errors.push_str(&format!(
                    "assign-key function `{}` input type should match `{}` provided by `{}` but found `{}`",
                    self.assign_key.uses,
                    expected_type.value.name,
                    &input_provider_name,
                    assign_key_input.type_.name
                ));
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_update_state(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        if let Some(update_state) = &self.update_state {
            update_state.validate_update_state(types)
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "parser")]
    pub fn update_inline_operators(&mut self) -> Result<()> {
        self.assign_key.update_signature_from_code()?;

        for step in &mut self.transforms.steps {
            step.update_signature_from_code()?;
        }

        if let Some(update_state) = &mut self.update_state {
            update_state.update_signature_from_code()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use crate::{
        metadata::io::topic::KVSchemaType,
        util::{sdf_types_map::SdfTypesMap, validation_error::ValidationError},
        wit::{
            dataflow::{PackageDefinition, PackageImport},
            io::TypeRef,
            metadata::{NamedParameter, Parameter, ParameterKind, SdfKeyedStateValue},
            operator::{
                PartitionOperator, StepInvocation, StepState, TransformOperator, Transforms,
            },
            package_interface::{FunctionImport, Header, OperatorType},
            states::{SdfKeyedState, State, StateTyped},
        },
    };

    fn packages() -> Vec<PackageDefinition> {
        vec![PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                name: "my-pkg".to_string(),
                namespace: "my-ns".to_string(),
                version: "0.1.0".to_string(),
            },
            functions: vec![map_fn(), assign_key_fn()],
            imports: vec![],
            types: vec![],
            states: vec![],
            dev: None,
        }]
    }

    fn map_fn() -> (StepInvocation, OperatorType) {
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

    fn assign_key_fn() -> (StepInvocation, OperatorType) {
        (
            StepInvocation {
                uses: "assign-key-fn".to_string(),
                inputs: vec![NamedParameter {
                    name: "word-count".to_string(),
                    type_: TypeRef {
                        name: "U8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "U8".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            OperatorType::AssignKey,
        )
    }

    fn imports() -> Vec<PackageImport> {
        vec![PackageImport {
            metadata: Header {
                name: "my-pkg".to_string(),
                namespace: "my-ns".to_string(),
                version: "0.1.0".to_string(),
            },
            functions: vec![
                FunctionImport {
                    name: "map-fn".to_string(),
                    alias: None,
                },
                FunctionImport {
                    name: "assign-key-fn".to_string(),
                    alias: None,
                },
            ],
            path: Some("path/to/my-pkg".to_string()),
            types: vec![],
            states: vec![],
        }]
    }

    fn partition_operator() -> PartitionOperator {
        PartitionOperator {
            assign_key: StepInvocation {
                uses: "assign-key-fn".to_string(),
                ..Default::default()
            },
            transforms: {
                Transforms {
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
                }
            },
            update_state: None,
        }
    }

    fn expected_type() -> KVSchemaType {
        (
            None,
            TypeRef {
                name: "s16".to_string(),
            },
        )
            .into()
    }

    #[test]
    fn test_import_operator_configs_merges_operator_signatures() {
        let mut states: BTreeMap<String, State> = Default::default();
        let mut partition = partition_operator();

        assert!(partition.assign_key.inputs.is_empty());
        assert!(partition.assign_key.output.is_none());

        let partition_steps = &partition.transforms.steps;

        assert!(partition_steps.first().unwrap().inner().inputs.is_empty());
        assert!(partition_steps.first().unwrap().inner().output.is_none());
        assert!(partition_steps.get(1).unwrap().inner().inputs.is_empty());
        assert!(partition_steps.get(1).unwrap().inner().output.is_none());

        assert!(states.is_empty());

        partition
            .import_operator_configs(&imports(), &packages(), &mut states)
            .unwrap();

        assert_eq!(partition.assign_key.inputs.len(), 1);
        assert!(partition.assign_key.output.is_some());

        let partition_steps = &partition.transforms.steps;

        assert_eq!(partition_steps.first().unwrap().inner().inputs.len(), 1);
        assert!(partition_steps.first().unwrap().inner().output.is_some());
        assert_eq!(partition_steps.get(1).unwrap().inner().inputs.len(), 1);
        assert!(partition_steps.get(1).unwrap().inner().output.is_some());

        assert_eq!(states.len(), 1);
    }

    #[test]
    fn test_validate_validates_assign_key_operator() {
        let types = SdfTypesMap::default();
        let mut partition = partition_operator();
        partition.assign_key.output = None;

        let res = partition
            .validate(&types, &expected_type(), "service transforms block")
            .expect_err("should fail for invalid assign key operator");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-key type function `assign-key-fn` requires an output type"
        )));
    }

    #[test]
    fn test_validate_validates_assign_key_input_matches_expected_input() {
        let types = SdfTypesMap::default();
        let mut partition = partition_operator();
        partition.assign_key.inputs = vec![NamedParameter {
            name: "value".to_string(),
            type_: TypeRef {
                name: "u8".to_string(),
            },
            optional: false,
            kind: ParameterKind::Value,
        }];

        let res = partition
            .validate(&types, &expected_type(), "service transforms block")
            .expect_err("should fail for assign key operator with wrong input type");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-key function `assign-key-fn` input type should match `s16` provided by `service transforms block` but found `u8`"
        )));
    }

    #[test]
    fn test_validate_validates_transforms() {
        let types = SdfTypesMap::default();
        let mut partition = partition_operator();

        partition.transforms = Transforms {
            steps: vec![TransformOperator::Filter(StepInvocation {
                uses: "filter-fn".to_string(),
                ..Default::default()
            })],
        };

        let res = partition
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for invalid filter function");

        assert!(res.errors.contains(&ValidationError::new(
            "transforms block is invalid: filter type function `filter-fn` should have exactly 1 input type, found 0"
        )));
    }

    #[test]
    fn test_operators() {
        let partition = partition_operator();

        let operators = partition.operators();

        assert_eq!(operators.len(), 3);
        assert_eq!(operators[0].0.uses, "assign-key-fn");
        assert_eq!(operators[1].0.uses, "map-fn");
        assert_eq!(operators[2].0.uses, "map-fn");
    }

    #[test]
    fn test_add_operator() {
        let mut partition = PartitionOperator {
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
        };

        let res = partition.add_operator(
            Some(1),
            OperatorType::Map,
            StepInvocation {
                uses: "prospect_map_prospect2".to_string(),
                ..Default::default()
            },
        );

        assert!(res.is_ok());
        assert_eq!(partition.transforms.steps.len(), 2);
    }

    #[test]
    fn test_delete_operator() {
        let mut partition = PartitionOperator {
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
        };

        let res = partition.delete_operator(Some(0));

        assert!(res.is_ok());
        assert_eq!(partition.transforms.steps.len(), 0);
    }
}
