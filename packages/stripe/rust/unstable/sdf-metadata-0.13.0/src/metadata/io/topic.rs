use crate::{
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
    },
    wit::{
        dataflow::Topic,
        io::TypeRef,
        metadata::{OutputType, SdfKeyValue},
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct TopicValidationFailure {
    pub name: String,
    pub errors: Vec<TopicValidationError>,
}

impl ConfigError for TopicValidationFailure {
    fn readable(&self, indents: usize) -> String {
        let mut result = format!(
            "{}Topic `{}` is invalid:\n",
            INDENT.repeat(indents),
            self.name
        );

        for error in &self.errors {
            result.push_str(&error.readable(indents + 1));
        }

        result
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum TopicValidationError {
    InvalidKeyRef(String),
    InvalidValueRef(String),
    Name(Vec<TopicNameError>),
    MissingConverter,
}

impl ConfigError for TopicValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::InvalidKeyRef(key) => {
                format!(
                    "{}Referenced key type `{}` not found in config or imported types\n",
                    indent, key
                )
            }
            Self::InvalidValueRef(value) => {
                format!(
                    "{}Referenced type `{}` not found in config or imported types\n",
                    indent, value
                )
            }
            Self::Name(errors) => {
                let mut result = format!("{}Topic name is invalid:\n", indent);

                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
            Self::MissingConverter => {
                format!("{}Topic needs to have a \"converter\" specified for serializing/deserializing records\n", indent)
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum TopicNameError {
    Empty,
    TooLong,
    InvalidChars,
    StartsOrEndsWithDash,
}

impl ConfigError for TopicNameError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::Empty => format!("{}Name cannot be empty\n", indent),
            Self::TooLong => format!(
                "{}Name is too long, Topic names may only have {MAX_TOPIC_NAME_LEN} characters\n",
                indent
            ),
            Self::InvalidChars => format!(
                "{}Name may only contain lowercase alphanumeric characters or '-'\n",
                indent
            ),
            Self::StartsOrEndsWithDash => {
                format!("{}Name cannot start or end with a dash\n", indent)
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct KVSchemaType {
    pub key: Option<TypeRef>,
    pub value: TypeRef,
}

impl KVSchemaType {
    pub fn timestamp() -> Self {
        Self {
            key: None,
            value: TypeRef {
                name: "s64".to_string(),
            },
        }
    }
}

impl From<(Option<TypeRef>, TypeRef)> for KVSchemaType {
    fn from((key, value): (Option<TypeRef>, TypeRef)) -> Self {
        Self { key, value }
    }
}

impl From<OutputType> for KVSchemaType {
    fn from(output_type: OutputType) -> Self {
        match output_type {
            OutputType::Ref(r) => Self {
                key: None,
                value: r,
            },
            OutputType::KeyValue(SdfKeyValue { key, value }) => Self {
                key: Some(key),
                value,
            },
        }
    }
}

const MAX_TOPIC_NAME_LEN: usize = 63;

impl Topic {
    pub fn validate(&self, types_map: &SdfTypesMap) -> Result<(), TopicValidationFailure> {
        let mut failure = TopicValidationFailure {
            name: self.name.clone(),
            errors: vec![],
        };

        if let Err(name_errors) = validate_topic_name(&self.name) {
            failure.errors.push(TopicValidationError::Name(name_errors));
        }

        if let Some(key) = &self.schema.key {
            if !types_map.contains_key(&key.type_.name) {
                // Important! if we extract a ValidationError trait, see if we want to impl a push_str to
                // to simplify things like this

                failure
                    .errors
                    .push(TopicValidationError::InvalidKeyRef(key.type_.name.clone()));
            }
        }

        if !types_map.contains_key(&self.schema.value.type_.name) {
            failure.errors.push(TopicValidationError::InvalidValueRef(
                self.schema.value.type_.name.clone(),
            ))
        }

        if self.schema.value.converter.is_none() {
            failure.errors.push(TopicValidationError::MissingConverter);
        }
        if failure.errors.is_empty() {
            Ok(())
        } else {
            Err(failure)
        }
    }

    pub fn type_(&self) -> KVSchemaType {
        (
            self.schema.key.as_ref().map(|key| key.type_.clone()),
            self.schema.value.type_.clone(),
        )
            .into()
    }
}

pub fn validate_topic_name(name: &str) -> Result<(), Vec<TopicNameError>> {
    let mut errors = vec![];

    if name.is_empty() {
        errors.push(TopicNameError::Empty);
    }

    if name.len() > MAX_TOPIC_NAME_LEN {
        errors.push(TopicNameError::TooLong);
    }

    if !name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
    {
        errors.push(TopicNameError::InvalidChars);
    }

    if name.ends_with('-') || name.starts_with('-') {
        errors.push(TopicNameError::StartsOrEndsWithDash);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod test {
    use crate::wit::io::{SchemaSerDe, TopicSchema, TypeRef, SerdeConverter::Json};
    use crate::util::config_error::ConfigError;

    use super::*;

    #[test]
    fn test_validate_topic_name_rejects_long_name() {
        let name = "a".repeat(MAX_TOPIC_NAME_LEN + 1);
        let res = validate_topic_name(&name).expect_err("should error for long name");

        assert!(res.contains(&TopicNameError::TooLong));

        assert_eq!(
            res[0].readable(0),
            "Name is too long, Topic names may only have 63 characters\n"
        )
    }

    #[test]
    fn test_validate_topic_name_rejects_non_alphanumeric_name() {
        let name = "invalid-to&pic-name";
        let res = validate_topic_name(name).expect_err("should error for invalid name");

        assert!(res.contains(&TopicNameError::InvalidChars));

        assert_eq!(
            res[0].readable(0),
            "Name may only contain lowercase alphanumeric characters or '-'\n"
        )
    }

    #[test]
    fn test_validate_topic_name_rejects_name_starting_with_dash() {
        let name = "-invalid-topic-name";
        let res = validate_topic_name(name).expect_err("should error for invalid name");

        assert!(res.contains(&TopicNameError::StartsOrEndsWithDash));
        assert_eq!(res[0].readable(0), "Name cannot start or end with a dash\n")
    }

    #[test]
    fn test_validate_topic_name_rejects_name_ending_with_dash() {
        let name = "invalid-topic-name-";
        let res = validate_topic_name(name).expect_err("should error for invalid name");

        assert!(res.contains(&TopicNameError::StartsOrEndsWithDash));
        assert_eq!(res[0].readable(0), "Name cannot start or end with a dash\n")
    }

    #[test]
    fn test_validate_rejects_invalid_topic_name() {
        let types_map = SdfTypesMap::default();

        let topic = Topic {
            name: "invalid-to&pic-name".to_string(),
            schema: TopicSchema {
                key: None,
                value: SchemaSerDe {
                    converter: Some(Json),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                },
            },
            consumer: None,
            producer: None,
            profile: None,
        };

        let res = topic
            .validate(&types_map)
            .expect_err("should error for invalid name");

        assert!(res.errors.contains(&TopicValidationError::Name(vec![
            TopicNameError::InvalidChars
        ])));
        assert_eq!(
            res.readable(0),
            r#"Topic `invalid-to&pic-name` is invalid:
    Topic name is invalid:
        Name may only contain lowercase alphanumeric characters or '-'
"#
        )
    }

    #[test]
    fn test_validate_rejects_invalid_record_key_datatype() {
        let types_map = SdfTypesMap::default();

        let topic = Topic {
            name: "topic-name".to_string(),
            schema: TopicSchema {
                key: Some(SchemaSerDe {
                    converter: Some(Json),
                    type_: TypeRef {
                        name: "foobar".to_string(),
                    },
                }),
                value: SchemaSerDe {
                    converter: Some(Json),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                },
            },
            consumer: None,
            producer: None,
            profile: None,
        };

        let res = topic
            .validate(&types_map)
            .expect_err("should error for invalid record key type");

        assert!(res
            .errors
            .contains(&TopicValidationError::InvalidKeyRef("foobar".to_string())));
        assert_eq!(
            res.readable(0),
            r#"Topic `topic-name` is invalid:
    Referenced key type `foobar` not found in config or imported types
"#
        )
    }

    #[test]
    fn test_validate_rejects_invalid_record_value_datatype() {
        let types_map = SdfTypesMap::default();

        let topic = Topic {
            name: "topic-name".to_string(),
            schema: TopicSchema {
                key: None,
                value: SchemaSerDe {
                    converter: Some(Json),
                    type_: TypeRef {
                        name: "foobar".to_string(),
                    },
                },
            },
            consumer: None,
            producer: None,
            profile: None,
        };

        let res = topic
            .validate(&types_map)
            .expect_err("should error for invalid record type");

        assert!(res
            .errors
            .contains(&TopicValidationError::InvalidValueRef("foobar".to_string())));
        assert_eq!(
            res.readable(0),
            r#"Topic `topic-name` is invalid:
    Referenced type `foobar` not found in config or imported types
"#
        )
    }

    #[test]
    fn test_validate_rejects_topics_with_missing_converter() {
        let types_map = SdfTypesMap::default();

        let topic = Topic {
            name: "topic-name".to_string(),
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
        };

        let res = topic
            .validate(&types_map)
            .expect_err("should error missing converter");

        assert!(res.errors.contains(&TopicValidationError::MissingConverter));
        assert_eq!(
            res.readable(0),
            r#"Topic `topic-name` is invalid:
    Topic needs to have a "converter" specified for serializing/deserializing records
"#
        )
    }

    #[test]
    fn test_validate_accepts_valid_topic() {
        let types_map = SdfTypesMap::default();

        let topic = Topic {
            name: "topic-name".to_string(),
            schema: TopicSchema {
                key: Some(SchemaSerDe {
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    converter: Some(Json),
                }),
                value: SchemaSerDe {
                    converter: Some(Json),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                },
            },
            consumer: None,
            producer: None,
            profile: None,
        };

        topic.validate(&types_map).expect("should validate");
    }
}
