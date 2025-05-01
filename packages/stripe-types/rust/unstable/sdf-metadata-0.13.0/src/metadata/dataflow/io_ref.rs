use crate::{
    metadata::{io::topic::KVSchemaType, operator::transforms::validate_transforms_steps},
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
        validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::{IoRef, IoType, ScheduleConfig, Topic},
        operator::TransformOperator,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct IoRefValidationFailure {
    pub name: String,
    pub errors: Vec<IoRefValidationError>,
}

impl ConfigError for IoRefValidationFailure {
    fn readable(&self, indents: usize) -> String {
        self.errors
            .iter()
            .map(|e| e.readable(indents))
            .collect::<Vec<String>>()
            .join("")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum IoRefValidationError {
    NoTarget,
    InvalidRef(String),
    MissingTransformsInput,
    InvalidTransformsBlock(Vec<ValidationError>),
    InvalidOperator(Vec<ValidationError>),
}

impl ConfigError for IoRefValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::NoTarget => {
                format!("{}Cannot have a source with no target\n", indent)
            }
            Self::InvalidRef(id) => {
                format!("{}Referenced topic `{}` not found\n", indent, id)
            }
            Self::MissingTransformsInput => {
                format!(
                    "{}The first operator in a transforms block must take an input\n",
                    indent
                )
            }
            Self::InvalidTransformsBlock(errors) => {
                let mut res = format!("{}Transforms block is invalid:\n", indent);

                for error in errors {
                    res.push_str(&error.readable(indents + 1));
                }

                res
            }
            Self::InvalidOperator(errors) => {
                let mut res = format!("{}Invalid operator(s):\n", indent);

                for error in errors {
                    res.push_str(&error.readable(indents + 1));
                }

                res
            }
        }
    }
}

impl IoRef {
    pub fn schema_type(
        &self,
        topics: &[(String, Topic)],
    ) -> Result<Option<KVSchemaType>, IoRefValidationError> {
        match self.type_ {
            IoType::NoTarget => Ok(None),
            IoType::Topic => {
                let topic = topics.iter().find(|(id, _)| id == &self.id);

                match topic {
                    Some((_name, topic)) => Ok(Some(topic.type_())),
                    None => Err(IoRefValidationError::InvalidRef(self.id.clone())),
                }
            }
            IoType::Schedule => Ok(Some(KVSchemaType::timestamp())),
        }
    }

    pub fn source_type(
        &self,
        topics: &[(String, Topic)],
    ) -> Result<KVSchemaType, IoRefValidationError> {
        if let Some(last_step) = self.steps.last() {
            get_transform_chain_output_from_last_step(last_step)
                .map_err(|e| IoRefValidationError::InvalidTransformsBlock(vec![e]))
        } else {
            match self.schema_type(topics) {
                Ok(None) => Err(IoRefValidationError::NoTarget),
                Err(e) => Err(e),
                Ok(Some(valid_type)) => Ok(valid_type),
            }
        }
    }

    pub fn sink_type(
        &self,
        topics: &[(String, Topic)],
    ) -> Result<Option<KVSchemaType>, IoRefValidationError> {
        if let Some(first_step) = self.steps.first() {
            if let Some(input_type) = first_step.input_type() {
                Ok(Some(input_type))
            } else {
                Err(IoRefValidationError::InvalidTransformsBlock(vec![
                    ValidationError::new(
                        "The first operator in a transforms block must take an input",
                    ),
                ]))
            }
        } else {
            match self.schema_type(topics) {
                Ok(None) => Ok(None),
                Err(e) => Err(e),
                valid_type => valid_type,
            }
        }
    }

    fn validate_schedule_defined(
        &self,
        schedules: Option<&[ScheduleConfig]>,
    ) -> Result<(), IoRefValidationError> {
        if self.type_ == IoType::Schedule {
            let schedules = schedules.unwrap_or_default();
            if schedules.iter().any(|s| s.name == self.id) {
                Ok(())
            } else {
                Err(IoRefValidationError::InvalidRef(self.id.clone()))
            }
        } else {
            Ok(())
        }
    }

    pub fn validate_source(
        &self,
        types: &SdfTypesMap,
        topics: &[(String, Topic)],
        schedules: Option<&[ScheduleConfig]>,
    ) -> Result<(), IoRefValidationFailure> {
        let mut failure = IoRefValidationFailure {
            name: self.id.clone(),
            errors: vec![],
        };

        if let Err(e) = self.source_type(topics) {
            failure.errors.push(e);
        }

        if let Ok(Some(topic_type)) = self.schema_type(topics) {
            if !self.steps.is_empty() {
                if let Err(e) = self.validate_source_or_sink_steps(
                    types,
                    &topic_type,
                    format!("Topic `{}`", self.id),
                ) {
                    failure
                        .errors
                        .push(IoRefValidationError::InvalidOperator(e.errors))
                }
            }
        }

        if let Err(err) = self.validate_schedule_defined(schedules) {
            failure.errors.push(err);
        }

        if failure.errors.is_empty() {
            Ok(())
        } else {
            Err(failure)
        }
    }

    pub fn validate_sink(
        &self,
        types: &SdfTypesMap,
        topics: &[(String, Topic)],
        service_output_type: &KVSchemaType,
    ) -> Result<(), IoRefValidationFailure> {
        let mut failure = IoRefValidationFailure {
            name: self.id.clone(),
            errors: vec![],
        };
        let mut transforms_errors = vec![];

        match self.sink_type(topics) {
            Err(e) => failure.errors.push(e),
            Ok(Some(sink_ty)) => {
                if sink_ty.value.name.replace('-', "_")
                    != service_output_type.value.name.replace('-', "_")
                {
                    transforms_errors.push(ValidationError::new(&format!(
                        "service output type `{}` does not match sink input type `{}`",
                        service_output_type.value.name, sink_ty.value.name
                    )));
                }

                if let (Some(sink_key), Some(service_key)) = (sink_ty.key, &service_output_type.key)
                {
                    if sink_key != *service_key {
                        transforms_errors.push(
                            ValidationError::new(&format!(
                                "sink transforms input key type `{}` does not match service output key type `{}`",
                                sink_key.name,
                                service_key.name
                            ))
                        );
                    }
                }
            }
            Ok(None) => {}
        }

        if let Some(last_step) = self.steps.last() {
            if let Err(e) = self.validate_source_or_sink_steps(
                types,
                service_output_type,
                "service".to_string(),
            ) {
                failure
                    .errors
                    .push(IoRefValidationError::InvalidOperator(e.errors));
            }

            match get_transform_chain_output_from_last_step(last_step) {
                Ok(output_ty) => match self.schema_type(topics) {
                    Ok(Some(topic_type)) => {
                        if topic_type.value != output_ty.value {
                            transforms_errors.push(ValidationError::new(&format!(
                                    "transforms steps final output type `{}` does not match topic type `{}`",
                                    output_ty.value.name,
                                    topic_type.value.name
                                )));
                        }

                        if let Some(topic_key) = topic_type.key {
                            if let Some(output_key) = output_ty.key {
                                if topic_key != output_key {
                                    transforms_errors.push(ValidationError::new(&format!(
                                            "sink `{}` has transforms steps but final output key type `{}` does not match topic key type `{}`",
                                            self.id,
                                            output_key.name,
                                            topic_key.name
                                        )));
                                }
                            }
                        }
                    }
                    Ok(None) => transforms_errors.push(ValidationError::new(
                        "sink cannot have transforms steps without a target",
                    )),
                    _ => {}
                },
                Err(e) => {
                    transforms_errors.push(e);
                }
            }
        }

        if !transforms_errors.is_empty() {
            failure
                .errors
                .push(IoRefValidationError::InvalidTransformsBlock(
                    transforms_errors,
                ));
        }

        if failure.errors.is_empty() {
            Ok(())
        } else {
            Err(failure)
        }
    }

    pub fn validate_source_or_sink_steps(
        &self,
        types: &SdfTypesMap,
        expected_input_type: &KVSchemaType,
        input_provider_description: String,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(transforms_errors) = validate_transforms_steps(
            &self.steps,
            types,
            expected_input_type.clone(),
            input_provider_description,
        ) {
            errors.concat(&transforms_errors);
        };

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }
}

// Helper function to get the output type of the last step in a transform chain
// If the last step is a filter, its input type is returned
fn get_transform_chain_output_from_last_step(
    last_step: &TransformOperator,
) -> Result<KVSchemaType, ValidationError> {
    match last_step {
        // if filter, use input type
        TransformOperator::Filter(_) => {
            if let Some(input_type) = last_step.input_type() {
                Ok(input_type)
            } else {
                Err(ValidationError::new(
                    "Last transforms step is invalid. Filter operator should have an input type",
                ))
            }
        }
        _ => {
            if let Some(output_type) = last_step.output_type() {
                Ok(output_type)
            } else {
                Err(ValidationError::new(
                    "Last transforms step is invalid. Expected an operator with an output type",
                ))
            }
        }
    }
}
#[cfg(test)]
mod test {
    use crate::{
        metadata::dataflow::io_ref::IoRefValidationError,
        util::{
            config_error::ConfigError, sdf_types_map::SdfTypesMap,
            validation_error::ValidationError,
        },
        wit::{
            dataflow::{IoRef, IoType, Topic, TransformOperator},
            io::{SchemaSerDe, TopicSchema, TypeRef},
            metadata::{NamedParameter, OutputType, Parameter, ParameterKind},
            operator::StepInvocation,
        },
    };

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
        ]
    }

    #[test]
    fn test_topic_type_returns_none_for_no_target() {
        let io_ref = IoRef {
            id: "no-target".to_string(),
            type_: IoType::NoTarget,
            steps: vec![],
        };

        assert_eq!(io_ref.schema_type(&[]), Ok(None));
    }

    #[test]
    fn test_topic_type_returns_error_when_topic_not_found() {
        let io_ref = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![],
        };

        assert_eq!(
            io_ref.schema_type(&[]),
            Err(IoRefValidationError::InvalidRef("my-topic".to_string()))
        );
    }

    #[test]
    fn test_topic_type_returns_topic_type() {
        let io_ref = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![],
        };

        let topics = vec![(
            "my-topic".to_string(),
            Topic {
                name: "my-topic".to_string(),
                schema: TopicSchema {
                    key: None,
                    value: SchemaSerDe {
                        type_: TypeRef {
                            name: "string".to_string(),
                        },
                        converter: None,
                    },
                },
                consumer: None,
                producer: None,
                profile: None,
            },
        )];

        assert_eq!(
            io_ref.schema_type(&topics),
            Ok(Some(
                (
                    None,
                    TypeRef {
                        name: "string".to_string()
                    }
                )
                    .into()
            ))
        );
    }

    #[test]
    fn test_validate_source_or_sink_steps_validates_steps_as_a_transforms() {
        let types = SdfTypesMap::default();
        let io_ref = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![TransformOperator::Map(StepInvocation {
                uses: "my-function".to_string(),
                output: None,
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

        let res = io_ref
            .validate_source_or_sink_steps(
                &types,
                &expected_input_type,
                "topic `my-topic`".to_string(),
            )
            .expect_err("should error for invalid step");

        assert!(res.errors.contains(&ValidationError::new(
            "map type function `my-function` should have exactly 1 input type, found 0"
        )));
    }

    #[test]
    fn test_validate_source_rejects_source_when_topic_not_found() {
        let types = SdfTypesMap::default();
        let source = IoRef {
            type_: IoType::Topic,
            id: "my-source-topic".to_string(),
            steps: vec![],
        };

        let res = source
            .validate_source(&types, &[], None)
            .expect_err("should error for missing sources");

        assert!(res.errors.contains(&IoRefValidationError::InvalidRef(
            "my-source-topic".to_string()
        )));
    }

    #[test]
    fn test_validate_source_rejects_source_when_last_step_has_no_output() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let source = IoRef {
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
                output: None,
                ..Default::default()
            })],
        };

        let res = source
            .validate_source(&types, &topics, None)
            .expect_err("should error for invalid step");

        assert!(res
            .errors
            .contains(&IoRefValidationError::InvalidTransformsBlock(vec![
                ValidationError::new(
                    "Last transforms step is invalid. Expected an operator with an output type"
                )
            ])));

        assert!(res.readable(0).contains(
            r#"Transforms block is invalid:
    Last transforms step is invalid. Expected an operator with an output type
"#
        ));
    }

    #[test]
    fn test_validate_source_rejects_source_when_topic_is_no_target() {
        let types = SdfTypesMap::default();
        let source = IoRef {
            type_: IoType::NoTarget,
            id: "".to_string(),
            steps: vec![],
        };

        let res = source
            .validate_source(&types, &[], None)
            .expect_err("should error for missing sources");

        assert!(res.errors.contains(&IoRefValidationError::NoTarget));

        assert_eq!(
            res.readable(0),
            r#"Cannot have a source with no target
"#
        );
    }

    #[test]
    fn test_validate_source_validates_source_transforms() {
        let types = SdfTypesMap::default();
        let source = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
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
                output: None,
                ..Default::default()
            })],
        };
        let topics = topics();

        let res = source
            .validate_source(&types, &topics, None)
            .expect_err("should error for invalid step");

        assert!(res
            .errors
            .contains(&IoRefValidationError::InvalidOperator(vec![
                ValidationError::new("map type function `my-function` requires an output type")
            ])));

        assert_eq!(
            res.readable(0),
            r#"Transforms block is invalid:
    Last transforms step is invalid. Expected an operator with an output type
Invalid operator(s):
    map type function `my-function` requires an output type
"#
        );
    }

    #[test]
    fn test_validate_source_accepts_valid_sources() {
        let types = SdfTypesMap::default();
        let io_ref = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
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
                        type_: OutputType::Ref(TypeRef {
                            name: "u16".to_string(),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                TransformOperator::Map(StepInvocation {
                    uses: "my-other-function".to_string(),
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
                }),
            ],
        };

        io_ref
            .validate_source(&types, &topics(), None)
            .expect("should validate");
    }

    #[test]
    fn test_validate_sink_rejects_sink_when_topic_not_found() {
        let types = SdfTypesMap::default();
        let sink = IoRef {
            type_: IoType::Topic,
            id: "my-sink-topic".to_string(),
            steps: vec![],
        };

        let res = sink
            .validate_sink(
                &types,
                &[],
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for missing sources");

        assert!(res.errors.contains(&IoRefValidationError::InvalidRef(
            "my-sink-topic".to_string()
        )));

        assert_eq!(
            res.readable(0),
            r#"Referenced topic `my-sink-topic` not found
"#
        );
    }

    #[test]
    fn test_validate_sink_rejects_sink_when_first_step_has_no_input() {
        let topics = topics();
        let types = SdfTypesMap::default();

        let sink = IoRef {
            type_: IoType::Topic,
            id: "my-other-topic".to_string(),
            steps: vec![TransformOperator::Map(StepInvocation {
                uses: "my-function".to_string(),
                inputs: vec![],
                ..Default::default()
            })],
        };

        let res = sink
            .validate_sink(
                &types,
                &topics,
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for invalid step");

        assert!(res
            .errors
            .contains(&IoRefValidationError::InvalidTransformsBlock(vec![
                ValidationError::new("The first operator in a transforms block must take an input")
            ])));

        assert!(res.readable(0).contains(
            r#"Transforms block is invalid:
    The first operator in a transforms block must take an input
"#
        ));
    }

    #[test]
    fn test_validate_sink_validates_sink_transforms() {
        let types = SdfTypesMap::default();
        let sink = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![
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
                            name: "string".to_string(),
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
            ],
        };
        let topics = topics();

        let res = sink
            .validate_sink(
                &types,
                &topics,
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for invalid step");

        assert!(res.errors.iter().any(|e| {
            if let IoRefValidationError::InvalidOperator(transforms_errors) = e {
                if transforms_errors.contains(&ValidationError::new("Function `my-other-function` input type was expected to match `string` type provided by function `my-function`, but `u8` was found.")) {
                    return true
                }
            }

            false
        }));
    }

    #[test]
    fn test_validate_sink_validates_last_transforms_step_matches_topic() {
        let types = SdfTypesMap::default();
        let sink = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![TransformOperator::Map(StepInvocation {
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
                        name: "string".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            })],
        };
        let topics = topics();

        let res = sink
            .validate_sink(
                &types,
                &topics,
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for invalid step");

        assert!(res
            .errors
            .contains(&IoRefValidationError::InvalidTransformsBlock(vec![
                ValidationError::new(
                    "transforms steps final output type `string` does not match topic type `u8`"
                )
            ])));

        assert!(res.readable(0).contains(
            r#"Transforms block is invalid:
    transforms steps final output type `string` does not match topic type `u8`
"#
        ));
    }

    #[test]
    fn test_validate_sink_rejects_transforms_without_output() {
        let types = SdfTypesMap::default();
        let sink = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![TransformOperator::Map(StepInvocation {
                uses: "my-function".to_string(),
                inputs: vec![NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: None,
                ..Default::default()
            })],
        };
        let topics = topics();

        let res = sink
            .validate_sink(
                &types,
                &topics,
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for invalid step");

        assert!(res.errors.iter().any(|e| {
            if let IoRefValidationError::InvalidTransformsBlock(transforms_errors) = e {
                transforms_errors.contains(&ValidationError::new(
                    "Last transforms step is invalid. Expected an operator with an output type",
                ))
            } else {
                false
            }
        }));

        assert!(res.readable(0).contains(
            r#"Transforms block is invalid:
    Last transforms step is invalid. Expected an operator with an output type
"#
        ));
    }

    #[test]
    fn test_validate_sink_rejects_transforms_without_a_sink_target() {
        let types = SdfTypesMap::default();
        let sink = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::NoTarget,
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
        };
        let topics = topics();

        let res = sink
            .validate_sink(
                &types,
                &topics,
                &(
                    Some(TypeRef {
                        name: "bytes".to_string(),
                    }),
                    TypeRef {
                        name: "string".to_string(),
                    },
                )
                    .into(),
            )
            .expect_err("should error for invalid step");

        assert!(res.errors.iter().any(|e| {
            if let IoRefValidationError::InvalidTransformsBlock(transforms_errors) = e {
                if transforms_errors.contains(&ValidationError::new(
                    "sink cannot have transforms steps without a target",
                )) {
                    return true;
                }
            }

            false
        }));
    }

    #[test]
    fn test_validate_sink_accepts_valid_sinks() {
        let types = SdfTypesMap::default();
        let io_ref = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
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
                            name: "u16".to_string(),
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
                }),
            ],
        };

        io_ref
            .validate_sink(
                &types,
                &topics(),
                &(
                    None,
                    TypeRef {
                        name: "u8".to_string(),
                    },
                )
                    .into(),
            )
            .expect("should validate");
    }

    #[test]
    fn test_get_transform_chain_output_from_last_step_if_filter() {
        let last_step: crate::wit::operator::TransformOperator =
            TransformOperator::Filter(StepInvocation {
                uses: "my-filter".to_string(),
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
                        name: "bool".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            });

        let res = super::get_transform_chain_output_from_last_step(&last_step)
            .expect("should return output type");

        assert_eq!(
            res,
            (
                None,
                TypeRef {
                    name: "u8".to_string()
                }
            )
                .into()
        );
    }

    #[test]
    fn test_get_transform_chain_output_from_last_step_if_map() {
        let last_step: crate::wit::operator::TransformOperator =
            TransformOperator::Map(StepInvocation {
                uses: "my-map".to_string(),
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
            });

        let res = super::get_transform_chain_output_from_last_step(&last_step)
            .expect("should return output type");

        assert_eq!(
            res,
            (
                None,
                TypeRef {
                    name: "u16".to_string()
                }
            )
                .into()
        );
    }

    #[test]
    fn test_source_type_returns_type_when_last_step_is_filter() {
        let source = IoRef {
            id: "my-source".to_string(),
            type_: IoType::Topic,
            steps: vec![TransformOperator::Filter(StepInvocation {
                uses: "my-filter".to_string(),
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
                        name: "bool".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            })],
        };

        let topics = topics();

        let res = source
            .source_type(&topics)
            .expect("should return output type");

        assert_eq!(
            res,
            (
                None,
                TypeRef {
                    name: "u8".to_string()
                }
            )
                .into()
        );
    }

    #[test]
    fn test_source_type_returns_type_when_last_step_is_map() {
        let source = IoRef {
            id: "my-source".to_string(),
            type_: IoType::Topic,
            steps: vec![TransformOperator::Map(StepInvocation {
                uses: "my-map".to_string(),
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
        };

        let topics = topics();

        let res = source
            .source_type(&topics)
            .expect("should return output type");

        assert_eq!(
            res,
            (
                None,
                TypeRef {
                    name: "u16".to_string()
                }
            )
                .into()
        );
    }

    #[test]
    fn test_validate_schedule_undefined() {
        let source = IoRef {
            id: "my-schedule".to_string(),
            type_: IoType::Schedule,
            steps: vec![],
        };

        let error = source
            .validate_schedule_defined(None)
            .expect_err("should error for undefined schedule");

        assert_eq!(
            error,
            IoRefValidationError::InvalidRef("my-schedule".to_string())
        );
    }

    #[test]
    fn test_source_type_returns_topic_type_with_no_steps() {
        let source = IoRef {
            id: "my-topic".to_string(),
            type_: IoType::Topic,
            steps: vec![],
        };

        let topics = topics();

        let res = source
            .source_type(&topics)
            .expect("should return output type");

        assert_eq!(
            res,
            (
                None,
                TypeRef {
                    name: "u8".to_string()
                }
            )
                .into()
        );
    }
}
