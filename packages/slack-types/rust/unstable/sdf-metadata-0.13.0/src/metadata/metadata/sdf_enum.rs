use std::collections::HashSet;

use wit_encoder::{Variant, VariantCase};

use sdf_common::render::wit_name_case;

use crate::{
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
    },
    wit::metadata::SdfEnum,
};

use super::sdf_type::SdfTypeValidationError;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SdfEnumValidationError {
    EmptyName,
    EmptyRef,
    DuplicateVariantName(String),
    InvalidRef(String),
}

impl From<Vec<SdfEnumValidationError>> for SdfTypeValidationError {
    fn from(errors: Vec<SdfEnumValidationError>) -> Self {
        Self::SdfEnum(errors)
    }
}

impl ConfigError for SdfEnumValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::EmptyName => {
                format!("{}Enum variant name cannot be empty\n", indent)
            }
            Self::EmptyRef => {
                format!("{}Enum variant does not reference any type\n", indent)
            }
            Self::InvalidRef(name) => {
                format!(
                    "{}Referenced type `{}` not found in config or imported types\n",
                    indent, name
                )
            }
            Self::DuplicateVariantName(name) => {
                format!("{}Duplicate enum variant name `{}`\n", indent, name)
            }
        }
    }
}

impl SdfEnum {
    pub fn validate(&self, map: &SdfTypesMap) -> Result<(), Vec<SdfEnumValidationError>> {
        let mut errors = vec![];
        let mut variant_names = HashSet::new();

        for ty in self.variants.iter() {
            if ty.name.is_empty() {
                errors.push(SdfEnumValidationError::EmptyName);
            }

            if variant_names.contains(&ty.name) {
                errors.push(SdfEnumValidationError::DuplicateVariantName(
                    ty.name.clone(),
                ));
            } else {
                variant_names.insert(&ty.name);
            }

            if let Some(type_ref) = &ty.value {
                if type_ref.name.is_empty() {
                    errors.push(SdfEnumValidationError::EmptyRef);
                } else if !map.contains_key(&type_ref.name) {
                    errors.push(SdfEnumValidationError::InvalidRef(type_ref.name.clone()));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn wit_variant(&self) -> Variant {
        let mut variant = Variant::empty();
        for var in self.variants.iter() {
            let name = wit_name_case(&var.name);
            let case = if let Some(ref ty_ref) = var.value {
                let value = ty_ref.wit_type();
                VariantCase::value(name, value)
            } else {
                VariantCase::empty(name)
            };

            variant.case(case);
        }
        variant
    }
}

#[cfg(test)]
mod test {
    use crate::{
        metadata::metadata::sdf_enum::SdfEnumValidationError,
        util::{config_error::ConfigError, sdf_types_map::SdfTypesMap},
        wit::metadata::{EnumField, SdfEnum, SerdeConfig, TypeRef},
    };

    #[test]
    fn test_validate_accepts_valid_enums() {
        let map = SdfTypesMap::default();

        let sdf_enum = SdfEnum {
            tagging: None,
            variants: vec![EnumField {
                name: "my-u8".to_string(),
                value: Some(TypeRef {
                    name: "u8".to_string(),
                }),
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        sdf_enum.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_rejects_enum_variant_without_names() {
        let map = SdfTypesMap::default();

        let sdf_enum = SdfEnum {
            tagging: None,
            variants: vec![EnumField {
                name: "".to_string(),
                value: Some(TypeRef {
                    name: "u8".to_string(),
                }),
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        let res = sdf_enum
            .validate(&map)
            .expect_err("should error for empty variant name");

        assert!(res.contains(&SdfEnumValidationError::EmptyName));
        assert_eq!(res[0].readable(0), "Enum variant name cannot be empty\n")
    }

    #[test]
    fn test_validate_rejects_enum_variants_that_reference_nonexistent_types() {
        let map = SdfTypesMap::default();

        let sdf_enum = SdfEnum {
            tagging: None,
            variants: vec![EnumField {
                name: "my-type".to_string(),
                value: Some(TypeRef {
                    name: "foobar".to_string(),
                }),
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        let res = sdf_enum
            .validate(&map)
            .expect_err("should error invalid TypeRef");

        assert!(res.contains(&SdfEnumValidationError::InvalidRef("foobar".to_string())));
        assert_eq!(
            res[0].readable(0),
            "Referenced type `foobar` not found in config or imported types\n"
        )
    }

    #[test]
    fn test_validate_rejects_enums_with_duplicate_variant_names() {
        let map = SdfTypesMap::default();

        let sdf_enum = SdfEnum {
            tagging: None,
            variants: vec![
                EnumField {
                    name: "my-type".to_string(),
                    value: Some(TypeRef {
                        name: "string".to_string(),
                    }),
                    serde_config: SerdeConfig {
                        serialize: None,
                        deserialize: None,
                    },
                },
                EnumField {
                    name: "my-type".to_string(),
                    value: Some(TypeRef {
                        name: "u8".to_string(),
                    }),
                    serde_config: SerdeConfig {
                        serialize: None,
                        deserialize: None,
                    },
                },
            ],
        };

        let res = sdf_enum
            .validate(&map)
            .expect_err("should error duplicate variant names");

        assert!(res.contains(&SdfEnumValidationError::DuplicateVariantName(
            "my-type".to_string()
        )));
        assert_eq!(
            res[0].readable(0),
            "Duplicate enum variant name `my-type`\n"
        )
    }
}
