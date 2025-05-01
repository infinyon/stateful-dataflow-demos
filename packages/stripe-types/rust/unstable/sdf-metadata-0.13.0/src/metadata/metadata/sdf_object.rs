use std::collections::HashSet;

use wit_encoder::{Field, Record};

use sdf_common::render::wit_name_case;

use crate::{
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
    },
    wit::metadata::SdfObject,
};

use super::sdf_type::SdfTypeValidationError;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SdfObjectValidationError {
    EmptyName,
    DuplicateFieldName(String),
    InvalidRef(String),
}

impl From<Vec<SdfObjectValidationError>> for SdfTypeValidationError {
    fn from(errs: Vec<SdfObjectValidationError>) -> Self {
        Self::SdfObject(errs)
    }
}

impl ConfigError for SdfObjectValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::EmptyName => {
                format!("{}Field name cannot be empty\n", indent)
            }
            Self::DuplicateFieldName(name) => {
                format!("{}Duplicate field name `{}`\n", indent, name)
            }
            Self::InvalidRef(name) => {
                format!(
                    "{}Referenced type `{}` not found in config or imported types\n",
                    indent, name
                )
            }
        }
    }
}

impl SdfObject {
    pub fn validate(&self, map: &SdfTypesMap) -> Result<(), Vec<SdfObjectValidationError>> {
        let mut errors = vec![];
        let mut field_names = HashSet::new();

        for field in &self.fields {
            if field.name.is_empty() {
                errors.push(SdfObjectValidationError::EmptyName);
            }

            if !map.contains_key(&field.type_.name) {
                errors.push(SdfObjectValidationError::InvalidRef(
                    field.type_.name.clone(),
                ));
            }

            if field_names.contains(&field.name) {
                errors.push(SdfObjectValidationError::DuplicateFieldName(
                    field.name.clone(),
                ));
            } else {
                field_names.insert(&field.name);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn wit_record(&self) -> Record {
        let fields = self.fields.iter().map(|field| {
            let name = wit_name_case(&field.name);

            let ty = field.type_.wit_type();

            if field.optional {
                Field::new(name, wit_encoder::Type::option(ty))
            } else {
                Field::new(name, ty)
            }
        });

        Record::new(fields)
    }
}

#[cfg(test)]
mod test {
    use crate::wit::metadata::{ObjectField, SdfObject, TypeRef, SerdeConfig};
    use super::*;

    #[test]
    fn test_validate_accepts_valid_object() {
        let map = SdfTypesMap::default();

        let object = SdfObject {
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
                    name: "age".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    serde_config: SerdeConfig {
                        serialize: None,
                        deserialize: None,
                    },
                },
            ],
        };

        object.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_rejects_object_with_invalid_field_name() {
        let map = SdfTypesMap::default();

        let object = SdfObject {
            fields: vec![ObjectField {
                name: "".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        let res = object
            .validate(&map)
            .expect_err("should error empty field name");

        assert!(res.contains(&SdfObjectValidationError::EmptyName));
        assert_eq!(res[0].readable(0), "Field name cannot be empty\n")
    }

    #[test]
    fn test_validate_rejects_object_with_invalid_type_reference() {
        let map = SdfTypesMap::default();

        let object = SdfObject {
            fields: vec![ObjectField {
                name: "struct-field".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        let res = object
            .validate(&map)
            .expect_err("should error invalid TypeRef");

        assert!(res.contains(&SdfObjectValidationError::InvalidRef("foobar".to_string())));
        assert_eq!(
            res[0].readable(0),
            "Referenced type `foobar` not found in config or imported types\n"
        )
    }

    #[test]
    fn test_validate_rejects_duplicate_field_names() {
        let map = SdfTypesMap::default();

        let object = SdfObject {
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
                    name: "name".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    serde_config: SerdeConfig {
                        serialize: None,
                        deserialize: None,
                    },
                },
            ],
        };

        let res = object
            .validate(&map)
            .expect_err("should error duplicate field name");

        assert!(res.contains(&SdfObjectValidationError::DuplicateFieldName(
            "name".to_string()
        )));
        assert_eq!(res[0].readable(0), "Duplicate field name `name`\n")
    }
}
