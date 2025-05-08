use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::{
    importer::{
        function::{imported_assign_timestamp_config, imported_operator_config},
        states::inject_states,
    },
    metadata::io::topic::KVSchemaType,
    util::{
        sdf_types_map::SdfTypesMap, validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::{PackageDefinition, PackageImport, Transforms},
        operator::{
            OperatorType, StepInvocation, TumblingWindow, Window, WindowProperties, WindowKind,
            WatermarkConfig,
        },
        states::State,
    },
};

use super::transforms::validate_transforms_steps;

impl Window {
    pub fn operators(&self) -> Vec<(StepInvocation, OperatorType)> {
        let mut operators = vec![];

        operators.push((self.assign_timestamp.clone(), OperatorType::AssignTimestamp));

        for step in &self.transforms.steps {
            operators.push((step.inner().clone(), step.clone().into()));
        }

        if let Some(partition) = &self.partition {
            let p_op = partition.operators();
            operators.extend(p_op);
        }

        if let Some(flush) = &self.flush {
            operators.push((flush.clone(), OperatorType::WindowAggregate));
        }

        operators
    }

    pub(crate) fn add_operator(
        &mut self,
        index: Option<usize>,
        partition: bool,
        operator_type: OperatorType,
        function: StepInvocation,
    ) -> Result<()> {
        if partition {
            if let Some(partition) = &mut self.partition {
                partition.add_operator(index, operator_type, function)
            } else {
                Err(anyhow!("Cannot add operator. Window and parition were specified but window does not have a partition"))
            }
        } else {
            self.transforms
                .insert_operator(index, operator_type, function)
        }
    }

    pub(crate) fn delete_operator(&mut self, index: Option<usize>, partition: bool) -> Result<()> {
        if partition {
            if let Some(partition) = &mut self.partition {
                partition.delete_operator(index)
            } else {
                Err(anyhow!("Cannot delete operator. Window and parition were specified but window does not have a partition"))
            }
        } else if let Some(index) = index {
            self.transforms.delete_operator(index)
        } else {
            todo!("cannot delete assign timestamp unless it is made optional")
        }
    }

    pub(crate) fn import_operator_configs(
        &mut self,
        imports: &[PackageImport],
        packages: &[PackageDefinition],
        service_states: &mut BTreeMap<String, State>,
    ) -> Result<()> {
        if self.assign_timestamp.is_imported(imports) {
            self.assign_timestamp =
                imported_assign_timestamp_config(&self.assign_timestamp, imports, packages)?;
            inject_states(service_states, &self.assign_timestamp.states)?;
        }

        if let Some(ref mut flush) = self.flush {
            if flush.is_imported(imports) {
                return Err(anyhow!(
                    "Importing functions for `Flush` is not yet supported"
                ));
            }
        }

        for step in &mut self.transforms.steps {
            if step.is_imported(imports) {
                *step = imported_operator_config(step, imports, packages)?;
                inject_states(service_states, &step.inner().states)?;
            }
        }

        if let Some(ref mut partition) = self.partition {
            partition.import_operator_configs(imports, packages, service_states)?;
        }

        Ok(())
    }

    pub fn output_type(&self, input_type: KVSchemaType) -> Result<KVSchemaType, ValidationError> {
        let mut expected_type = input_type;

        let failure_message = Err(ValidationError::new(
            "could not get output type from invalid window",
        ));

        if let Ok(transforms_output) = self.transforms.output_type(expected_type.clone()) {
            expected_type = transforms_output;
        } else {
            return failure_message;
        }

        if let Some(partition) = &self.partition {
            if let Ok(partition_output) = partition.output_type(expected_type.clone()) {
                expected_type = partition_output;
            } else {
                return failure_message;
            }
        }

        if let Some(flush) = &self.flush {
            if let Some(flush_output) = &flush.output {
                expected_type = flush_output.type_.clone().into();
            } else {
                return failure_message;
            }
        }

        Ok(expected_type)
    }

    pub fn validate(
        &self,
        types: &SdfTypesMap,
        expected_input_type: &KVSchemaType,
        mut input_provider_name: &str,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        let mut expected_input_type = expected_input_type.clone();

        if let Err(assign_timestamp_error) =
            self.validate_assign_timestamp(types, &expected_input_type, input_provider_name)
        {
            errors.concat(&assign_timestamp_error);
        }

        if let Err(transforms_error) = validate_transforms_steps(
            &self.transforms.steps,
            types,
            expected_input_type.clone(),
            input_provider_name.to_string(),
        ) {
            errors.concat_with_context("transforms block is invalid:", &transforms_error);
        }

        // update expected type after validating transforms
        if let Ok(output_type) = self.transforms.output_type(expected_input_type.clone()) {
            expected_input_type = output_type;
            input_provider_name = "window";
        } else {
            return Err(errors);
        };

        // validate partition
        if let Some(partition) = &self.partition {
            if let Err(partition_error) =
                partition.validate(types, &expected_input_type, input_provider_name)
            {
                errors.concat_with_context("partition is invalid:", &partition_error);
            }
        }

        if let Some(flush) = &self.flush {
            if let Err(flush_error) = flush.validate_window_aggregate(types) {
                errors.concat_with_context("flush function is invalid:", &flush_error);
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_assign_timestamp(
        &self,
        types: &SdfTypesMap,
        expected_input_type: &KVSchemaType,
        input_provider_name: &str,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(assign_timestamp_error) = self.assign_timestamp.validate_assign_timestamp(types)
        {
            errors.concat(&assign_timestamp_error);
        }

        let value_param = if self.assign_timestamp.requires_key_param() {
            let key_param = self.assign_timestamp.inputs.first();

            if let Some(key_param) = key_param {
                if let Some(ref expected_key) = expected_input_type.key {
                    if key_param.type_.name != expected_key.name {
                        errors.push_str(&format!(
                            "assign-timestamp function `{}` input type should match `{}` provided by `{}` but found `{}`",
                            self.assign_timestamp.uses,
                            expected_key.name,
                            &input_provider_name,
                            key_param.type_.name
                        ));
                    }
                }
            }

            self.assign_timestamp.inputs.get(1)
        } else {
            self.assign_timestamp.inputs.first()
        };

        //assert assign timestamps first input matches the expected input type
        if let Some(assign_timestamp_input) = value_param {
            if assign_timestamp_input.type_.name != expected_input_type.value.name {
                errors.push_str(&format!(
                    "assign-timestamp function `{}` input type should match `{}` provided by `{}` but found `{}`",
                    self.assign_timestamp.uses,
                    expected_input_type.value.name,
                    &input_provider_name,
                    assign_timestamp_input.type_.name
                ));
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "parser")]
    pub fn update_inline_operators(&mut self) -> Result<()> {
        self.assign_timestamp.update_signature_from_code()?;

        for step in &mut self.transforms.steps {
            step.update_signature_from_code()?;
        }

        if let Some(partition) = &mut self.partition {
            partition.update_inline_operators()?;
        }

        if let Some(flush) = &mut self.flush {
            flush.update_signature_from_code()?;
        }

        Ok(())
    }
}

impl Default for Window {
    fn default() -> Self {
        Self {
            properties: WindowProperties {
                kind: WindowKind::Tumbling(TumblingWindow {
                    duration: 0,
                    offset: 0,
                }),
                watermark_config: WatermarkConfig::default(),
            },
            assign_timestamp: Default::default(),
            flush: None,
            transforms: Transforms { steps: vec![] },
            partition: None,
        }
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
                TumblingWindow, Window, WindowProperties, WindowKind, WatermarkConfig,
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
            functions: vec![map_fn(), assign_timestamp_fn(), assign_key_fn()],
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

    fn assign_timestamp_fn() -> (StepInvocation, OperatorType) {
        (
            StepInvocation {
                uses: "assign-timestamp-fn".to_string(),
                inputs: vec![
                    NamedParameter {
                        name: "value".to_string(),
                        type_: TypeRef {
                            name: "S64".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    },
                    NamedParameter {
                        name: "event-time".to_string(),
                        type_: TypeRef {
                            name: "String".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    },
                ],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "S64".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            OperatorType::AssignTimestamp,
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

    fn window() -> Window {
        Window {
            properties: WindowProperties {
                kind: WindowKind::Tumbling(TumblingWindow {
                    duration: 60,
                    offset: 10,
                }),
                watermark_config: WatermarkConfig::default(),
            },
            assign_timestamp: StepInvocation {
                uses: "assign-timestamp-fn".to_string(),
                ..Default::default()
            },
            flush: None,
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
            partition: Some(PartitionOperator {
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
            }),
        }
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
                FunctionImport {
                    name: "assign-timestamp-fn".to_string(),
                    alias: None,
                },
            ],
            path: Some("path/to/my-pkg".to_string()),
            types: vec![],
            states: vec![],
        }]
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
    fn test_import_operator_configs_merges_operators_signatures() {
        let mut window = window();
        let mut states: BTreeMap<String, State> = Default::default();

        assert!(window.assign_timestamp.inputs.is_empty());
        assert!(window.assign_timestamp.output.is_none());

        let steps = &window.transforms.steps;
        assert!(steps.first().unwrap().inner().inputs.is_empty());
        assert!(steps.first().unwrap().inner().output.is_none());
        assert!(steps.get(1).unwrap().inner().inputs.is_empty());
        assert!(steps.get(1).unwrap().inner().output.is_none());

        let partition = window.partition.as_ref().unwrap();
        assert!(partition.assign_key.inputs.is_empty());
        assert!(partition.assign_key.output.is_none());

        let partition_steps = &partition.transforms.steps;
        assert!(partition_steps.first().unwrap().inner().inputs.is_empty());
        assert!(partition_steps.first().unwrap().inner().output.is_none());
        assert!(partition_steps.get(1).unwrap().inner().inputs.is_empty());
        assert!(partition_steps.get(1).unwrap().inner().output.is_none());

        assert!(states.is_empty());

        window
            .import_operator_configs(&imports(), &packages(), &mut states)
            .unwrap();

        assert_eq!(window.assign_timestamp.inputs.len(), 2);
        assert!(window.assign_timestamp.output.is_some());

        let steps = &window.transforms.steps;
        assert_eq!(steps.first().unwrap().inner().inputs.len(), 1);
        assert!(steps.first().unwrap().inner().output.is_some());
        assert_eq!(steps.get(1).unwrap().inner().inputs.len(), 1);
        assert!(steps.get(1).unwrap().inner().output.is_some());

        let partition = window.partition.as_ref().unwrap();
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
    fn test_validate_validates_assign_timestamp_operator() {
        let types = SdfTypesMap::default();
        let mut window = window();
        window.assign_timestamp.output = None;

        let res = window
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for invalid assign timestamp operator");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-timestamp type function `assign-timestamp-fn` requires an output type"
        )));
    }

    #[test]
    fn test_validate_validates_assign_timestamp_operator_input_matches_expected_input() {
        let types = SdfTypesMap::default();
        let mut window = window();
        window.assign_timestamp.inputs = vec![NamedParameter {
            name: "value".to_string(),
            type_: TypeRef {
                name: "u8".to_string(),
            },
            optional: false,
            kind: ParameterKind::Value,
        }];

        let res = window
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for assign timestamp operator with wrong input type");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-timestamp function `assign-timestamp-fn` input type should match `s16` provided by `transforms block` but found `u8`"
        )));
    }

    #[test]
    fn test_validate_validates_tranforms() {
        let types = SdfTypesMap::default();
        let mut window = window();

        window.transforms = Transforms {
            steps: vec![TransformOperator::Filter(StepInvocation {
                uses: "filter-fn".to_string(),
                ..Default::default()
            })],
        };

        let res = window
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for invalid filter function");

        assert!(res.errors.contains(&ValidationError::new(
            "transforms block is invalid: filter type function `filter-fn` should have exactly 1 input type, found 0"
        )));
    }

    #[test]
    fn test_validate_validates_partition() {
        let types = SdfTypesMap::default();
        let mut window = window();

        window.transforms = Transforms { steps: vec![] };

        window.partition = Some(PartitionOperator {
            assign_key: StepInvocation {
                uses: "assign-key-fn".to_string(),
                ..Default::default()
            },
            transforms: Transforms {
                steps: vec![TransformOperator::Filter(StepInvocation {
                    uses: "filter-fn".to_string(),
                    inputs: vec![NamedParameter {
                        name: "filter-input".to_string(),
                        type_: TypeRef {
                            name: "u8".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    }],
                    ..Default::default()
                })],
            },
            update_state: None,
        });

        let res = window
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for invalid partition transforms input");

        let msg = r"partition is invalid: transforms block is invalid: Function `filter-fn` input type was expected to match `s16` type provided by window, but `u8` was found.";

        assert!(res.errors.contains(&ValidationError::new(msg)));
    }

    #[test]
    fn test_validate_validates_flush_as_window_aggregate() {
        let types = SdfTypesMap::default();
        let mut window = window();

        window.flush = Some(StepInvocation {
            uses: "flush-fn".to_string(),
            ..Default::default()
        });
        window.transforms = Transforms { steps: vec![] };

        let res = window
            .validate(&types, &expected_type(), "transforms block")
            .expect_err("should fail for invalid filter function");

        assert!(res.errors.contains(&ValidationError::new(
            "flush function is invalid: window-aggregate type function `flush-fn` requires an output type"
        )));
    }

    #[test]
    fn test_window_operators() {
        let window = window();
        let operators = window.operators();

        assert_eq!(operators.len(), 6);
        assert_eq!(operators.first().unwrap().0.uses, "assign-timestamp-fn");
        assert_eq!(operators.get(1).unwrap().0.uses, "map-fn");
        assert_eq!(operators.get(2).unwrap().0.uses, "map-fn");
        assert_eq!(operators.get(3).unwrap().0.uses, "assign-key-fn");
        assert_eq!(operators.get(4).unwrap().0.uses, "map-fn");
        assert_eq!(operators.get(5).unwrap().0.uses, "map-fn");
    }

    #[test]
    fn test_add_window_operator() {
        let mut window = window();

        let (function, operator_type) = map_fn();

        let res = window.add_operator(Some(0), false, operator_type, function);

        assert!(res.is_ok());
        assert_eq!(window.transforms.steps.len(), 3);
    }

    #[test]
    fn test_add_window_partition_operator() {
        let mut window = window();

        let (function, operator_type) = map_fn();

        let res = window.add_operator(Some(0), true, operator_type, function);

        let partition = window.partition.expect("partition should exist");

        assert!(res.is_ok());
        assert_eq!(partition.transforms.steps.len(), 3);
    }

    #[test]
    fn test_add_window_fails_when_partition_incorrectly_specified() {
        let mut window = window();
        window.partition = None;

        let (function, operator_type) = map_fn();
        let res = window.add_operator(Some(0), true, operator_type, function);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot add operator. Window and parition were specified but window does not have a partition"
        );
    }

    #[test]
    fn test_delete_window_operator() {
        let mut window = window();

        let res = window.delete_operator(Some(0), false);

        assert!(res.is_ok());
        assert_eq!(window.transforms.steps.len(), 1);
    }

    #[test]
    fn test_delete_window_partition_operator() {
        let mut window = window();

        let res = window.delete_operator(Some(0), true);

        let partition = window.partition.expect("partition should exist");

        assert!(res.is_ok());
        assert_eq!(partition.transforms.steps.len(), 1);
    }

    #[test]
    fn test_delete_window_fails_when_partition_incorrectly_specified() {
        let mut window = window();
        window.partition = None;

        let res = window.delete_operator(Some(0), true);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot delete operator. Window and parition were specified but window does not have a partition"
        );
    }
}
