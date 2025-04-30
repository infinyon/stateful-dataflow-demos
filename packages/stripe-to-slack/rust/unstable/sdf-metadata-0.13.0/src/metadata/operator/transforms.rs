use anyhow::{Result, anyhow};

use crate::{
    metadata::io::topic::KVSchemaType,
    util::{
        sdf_types_map::SdfTypesMap, validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::dataflow::{TransformOperator, Transforms},
    wit::package_interface::{StepInvocation, OperatorType},
};

#[allow(clippy::derivable_impls)]
impl Default for Transforms {
    fn default() -> Self {
        Self { steps: vec![] }
    }
}

impl Transforms {
    pub(crate) fn insert_operator(
        &mut self,
        index: Option<usize>,
        operator_type: OperatorType,
        step_invocation: StepInvocation,
    ) -> Result<()> {
        let index = match index {
            Some(index) => index,
            None => {
                return Err(anyhow!(
                    "Must provide transforms index to insert operator into transforms block"
                ));
            }
        };

        if index > self.steps.len() {
            return Err(anyhow!(
                "cannot insert operator into transforms block, index is out of bounds, len = {}",
                self.steps.len()
            ));
        }

        let operator = match TransformOperator::new(operator_type, step_invocation) {
            Some(operator) => operator,
            None => {
                return Err(anyhow!(
                    "OperatorType {:?} not supported for transforms operator",
                    operator_type
                ));
            }
        };

        self.steps.insert(index, operator);

        Ok(())
    }

    pub(crate) fn delete_operator(&mut self, index: usize) -> Result<()> {
        if index >= self.steps.len() {
            return Err(anyhow!(
                "cannot delete operator from transforms block, index is out of bounds, len = {}",
                self.steps.len()
            ));
        }

        self.steps.remove(index);

        Ok(())
    }

    // gets the output type of the transforms if it is valid, otherwise returns an opaque error
    pub fn output_type(
        &self,
        mut transform_input_type: KVSchemaType,
    ) -> Result<KVSchemaType, ValidationError> {
        let failure_message = Err(ValidationError::new(
            "could not get output type from invalid transforms",
        ));

        for step in &self.steps {
            let step_invocation = step.inner();

            let value = if step_invocation.requires_key_param() {
                if step_invocation.inputs.is_empty() {
                    return failure_message;
                }

                step_invocation.inputs.get(1)
            } else {
                step_invocation.inputs.first()
            };

            if let Some(input_type) = value {
                if input_type.type_.name.replace('-', "_")
                    != transform_input_type.value.name.replace('-', "_")
                {
                    return failure_message;
                }
            } else {
                return failure_message;
            }

            if let Some(output_type) = &step_invocation.output {
                if !matches!(&step, TransformOperator::Filter(_)) {
                    output_type
                        .type_
                        .value_type()
                        .clone_into(&mut transform_input_type.value);

                    if let Some(key_type) = output_type.type_.key_type() {
                        transform_input_type.key = Some(key_type.clone());
                    }
                }
            }
        }

        Ok(transform_input_type)
    }
}

pub fn validate_transforms_steps(
    steps: &[TransformOperator],
    types: &SdfTypesMap,
    mut expected_type: KVSchemaType,
    mut input_provider_name: String,
) -> Result<(), ValidationFailure> {
    let mut errors = ValidationFailure::new();

    for step in steps {
        if let Err(function_error) = step.validate(types) {
            errors.concat(&function_error);
        }
        let step_invocation = step.inner();

        let value = if step_invocation.requires_key_param() {
            if let Some(input_key) = step_invocation.inputs.first() {
                if let Some(ref key) = expected_type.key {
                    if input_key.type_.name.replace('-', "_") != key.name.replace('-', "_") {
                        errors.push_str(&format!(
                            "in `{}`, key type does not match expected key type. {} != {}",
                            step_invocation.uses, input_key.type_.name, key.name
                        ));
                    }
                } else {
                    errors.push_str(
                        &format!(
                            "{} function requires a key, but none was found. Make sure that you define the right key in the topic configuration",
                            step_invocation.uses
                        )
                    );
                }
            } else {
                errors.push_str(&format!(
                    "map type function `{}` should have at least 1 input type, found 0",
                    step.name()
                ));
            }
            step_invocation.inputs.get(1)
        } else {
            step_invocation.inputs.first()
        };

        if let Some(input_type) = value {
            if input_type.type_.name.replace('-', "_") != expected_type.value.name.replace('-', "_")
            {
                errors.push_str(&format!(
                    "Function `{}` input type was expected to match `{}` type provided by {}, but `{}` was found.",
                    step.name(),
                    expected_type.value.name,
                    input_provider_name,
                    input_type.type_.name
                ));
            }
        }

        if let Some(output_type) = &step.inner().output {
            if !matches!(step, TransformOperator::Filter(_)) {
                output_type
                    .type_
                    .value_type()
                    .clone_into(&mut expected_type.value);
                if let Some(key_ty) = output_type.type_.key_type() {
                    expected_type.key = Some(key_ty.clone());
                }
            }

            input_provider_name = format!("function `{}`", step.name());
        }
    }

    if errors.any() {
        Err(errors)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::validate_transforms_steps;
    use crate::{
        util::{sdf_types_map::SdfTypesMap, validation_error::ValidationError},
        wit::{
            dataflow::{TransformOperator, Transforms},
            io::TypeRef,
            metadata::{NamedParameter, Parameter, ParameterKind},
            operator::StepInvocation,
        },
    };

    #[test]
    fn test_validate_transforms_steps_rejects_operators_without_input_type() {
        let types = SdfTypesMap::default();
        let steps = vec![TransformOperator::Map(StepInvocation {
            uses: "my-function".to_string(),
            inputs: vec![],
            ..Default::default()
        })];

        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();

        let res = validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "topic `my-topic`".to_string(),
        )
        .expect_err("should error for invalid step");

        assert!(res.errors.contains(&ValidationError::new(
            "map type function `my-function` should have exactly 1 input type, found 0"
        )));
    }

    #[test]
    fn test_validate_transforms_steps_rejects_operators_when_first_input_type_does_not_match_passed_in_type(
    ) {
        let types = SdfTypesMap::default();
        let steps = vec![TransformOperator::Map(StepInvocation {
            uses: "my-function".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "u8".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            ..Default::default()
        })];

        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();

        let res = validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "Topic `my-topic`".to_string(),
        )
        .expect_err("should error for invalid step");

        assert!(res.errors.contains(&ValidationError::new("Function `my-function` input type was expected to match `string` type provided by Topic `my-topic`, but `u8` was found.")));
    }

    #[test]
    fn test_validate_transforms_steps_rejects_operators_when_input_type_does_not_match_last_output_type(
    ) {
        let types = SdfTypesMap::default();
        let steps = vec![
            TransformOperator::Map(StepInvocation {
                uses: "my-function".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
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
            }),
            TransformOperator::Map(StepInvocation {
                uses: "my-other-function".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                ..Default::default()
            }),
        ];
        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();
        let res = validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "Topic `my-topic`".to_string(),
        )
        .expect_err("should error for invalid input type");

        assert!(
            res.errors.contains(
                &ValidationError::new("Function `my-other-function` input type was expected to match `u8` type provided by function `my-function`, but `string` was found.")
            )
        );
    }

    #[test]
    fn test_validate_transforms_steps_rejects_operators_when_output_type_does_not_exist() {
        let types = SdfTypesMap::default();
        let steps = vec![TransformOperator::Map(StepInvocation {
            uses: "my-function".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
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
        })];

        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();

        let res = validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "Topic `my-topic`".to_string(),
        )
        .expect_err("should error for invalid output type");

        assert!(&res.errors.contains(&ValidationError::new(
            "function `my-function` has invalid output type, Referenced type `foobar` not found in config or imported types"
        )))
    }

    #[test]
    fn test_validate_transforms_steps_rejects_functions_with_invalid_signatures() {
        let types = SdfTypesMap::default();
        let steps = vec![TransformOperator::Filter(StepInvocation {
            uses: "my-function".to_string(),
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                optional: true,
            }),
            ..Default::default()
        })];

        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();
        let res = validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "Topic `my-topic`".to_string(),
        )
        .expect_err("should error for invalid signature for function");

        assert!(res.errors.contains(&ValidationError::new(
            "filter type function `my-function` requires an output type of `bool`, but found `u8`"
        )));
    }

    #[test]
    fn test_validate_transforms_steps_ignores_filter_when_validating_next_input_type() {
        let types = SdfTypesMap::default();
        let steps = vec![
            TransformOperator::Filter(StepInvocation {
                uses: "my-filter".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "bool".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            TransformOperator::Map(StepInvocation {
                uses: "my-map".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
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
        ];

        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();
        validate_transforms_steps(
            &steps,
            &types,
            expected_input_type,
            "Topic `my-topic`".to_string(),
        )
        .expect("should pass for valid transforms");
    }

    #[test]
    fn test_output_type_ignores_filter() {
        let transforms = Transforms {
            steps: vec![TransformOperator::Filter(StepInvocation {
                uses: "my-filter".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "bool".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            })],
        };
        let expected_input_type = (
            Some(TypeRef {
                name: "bytes".to_string(),
            }),
            TypeRef {
                name: "string".to_string(),
            },
        )
            .into();
        let res = transforms
            .output_type(expected_input_type)
            .expect("should pass for valid transforms");

        assert_eq!(res.value.name, "string".to_string());
    }

    #[test]
    fn test_delete_operator() {
        let mut transforms = Transforms {
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

        let res = transforms.delete_operator(0);

        assert!(res.is_ok());
        assert_eq!(transforms.steps.len(), 1);
    }

    #[test]
    fn test_delete_operator_errors_on_index_out_of_bounds() {
        let mut transforms = Transforms {
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

        let res = transforms.delete_operator(2);

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "cannot delete operator from transforms block, index is out of bounds, len = 2"
        );
    }
}
