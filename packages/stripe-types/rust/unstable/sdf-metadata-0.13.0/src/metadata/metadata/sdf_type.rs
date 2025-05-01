use anyhow::Result;
use sdf_common::{
    constants::VOID_TYPE_REPR,
    render::{map_wit_keyword, wit_name_case},
    version::ApiVersion,
};
use wit_encoder::{TypeDef, TypeDefKind};

use crate::{
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
    },
    wit::metadata::SdfType,
};

use super::{
    sdf_arrow_row::SdfArrowRowValidationError, sdf_enum::SdfEnumValidationError,
    sdf_keyed_state::SdfKeyedStateValidationError, sdf_object::SdfObjectValidationError,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SdfTypeValidationError {
    InvalidSyntax(String),
    InvalidRef(String),
    SdfEnum(Vec<SdfEnumValidationError>),
    SdfKeyedState(Vec<SdfKeyedStateValidationError>),
    SdfObject(Vec<SdfObjectValidationError>),
    SdfArrowRow(Vec<SdfArrowRowValidationError>),
}

impl ConfigError for SdfTypeValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::InvalidSyntax(name) => {
                format!("{}Invalid syntax for {}. Check that the internal attributes are properly defined\n", indent, name)
            }
            Self::InvalidRef(name) => format!(
                "{}Referenced type `{}` not found in config or imported types\n",
                indent, name
            ),
            Self::SdfEnum(errors) => {
                let mut result = format!("{}Enum type is invalid:\n", INDENT.repeat(indents));

                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
            Self::SdfKeyedState(errors) => {
                let mut result =
                    format!("{}Keyed state type is invalid:\n", INDENT.repeat(indents));
                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
            Self::SdfObject(errors) => {
                let mut result = format!("{}Object type is invalid:\n", INDENT.repeat(indents));
                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
            Self::SdfArrowRow(errors) => {
                let mut result = format!("{}Arrow row type is invalid:\n", INDENT.repeat(indents));
                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
        }
    }
}

pub(crate) const HASHABLE_PRIMITIVES: [SdfType; 12] = [
    SdfType::U8,
    SdfType::U16,
    SdfType::U32,
    SdfType::U64,
    SdfType::S8,
    SdfType::S16,
    SdfType::S32,
    SdfType::S64,
    SdfType::Bool,
    SdfType::String,
    SdfType::Float32,
    SdfType::Float64,
];

impl SdfType {
    pub fn validate(&self, map: &SdfTypesMap) -> Result<(), SdfTypeValidationError> {
        match self {
            Self::Null => {}
            Self::U8 => {}
            Self::U16 => {}
            Self::U32 => {}
            Self::U64 => {}
            Self::S8 => {}
            Self::S16 => {}
            Self::S32 => {}
            Self::S64 => {}
            Self::Float32 => {}
            Self::Float64 => {}
            Self::String => {}
            Self::Bool => {}
            Self::Bytes => {}
            Self::Enum(sdf_enum) => sdf_enum.validate(map)?,
            Self::KeyedState(sdf_keyed_state) => return Ok(sdf_keyed_state.validate(map)?),
            Self::List(sdf_list) => {
                if !map.contains_key(&sdf_list.item.name) {
                    return Err(ref_type_error(&sdf_list.item.name));
                }
            }
            Self::Option(sdf_option) => {
                if !map.contains_key(&sdf_option.value.name) {
                    return Err(ref_type_error(&sdf_option.value.name));
                }
            }
            Self::Object(sdf_object) => return Ok(sdf_object.validate(map)?),
            Self::Named(type_ref) => {
                // fail if referenced type is "enum", "object", "list", "option", "keyed-state", "arrow-row", or "key-value"
                if [
                    "enum",
                    "object",
                    "list",
                    "option",
                    "keyed-state",
                    "arrow-row",
                    "key-value",
                ]
                .contains(&type_ref.name.as_str())
                {
                    return Err(SdfTypeValidationError::InvalidSyntax(
                        type_ref.name.to_owned(),
                    ));
                }

                if !map.contains_key(&type_ref.name) {
                    return Err(ref_type_error(&type_ref.name));
                }
            }
            Self::ArrowRow(sdf_arrow_row) => return Ok(sdf_arrow_row.validate()?),
            Self::KeyValue(sdf_key_value) => {
                if !map.contains_key(&sdf_key_value.key.name) {
                    return Err(ref_type_error(&sdf_key_value.key.name));
                }

                if !map.contains_key(&sdf_key_value.value.name) {
                    return Err(ref_type_error(&sdf_key_value.value.name));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn is_hashable(&self, map: &SdfTypesMap) -> bool {
        if HASHABLE_PRIMITIVES.contains(self) {
            return true;
        }

        if let Self::Named(type_ref) = self {
            if let Some((ty, _)) = map.get(&type_ref.name) {
                return ty.is_hashable(map);
            }
        }

        false
    }

    pub fn ty(&self) -> &str {
        match self {
            Self::Null => VOID_TYPE_REPR,
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::S8 => "s8",
            Self::S16 => "s16",
            Self::S32 => "s32",
            Self::S64 => "s64",
            Self::Float32 => "f32",
            Self::Float64 => "f64",
            Self::Bool => "bool",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Named(named) => &named.name,
            Self::Enum(_) => "enum",
            Self::Object(_) => "object",
            Self::List(_) => "list",
            Self::Option(_) => "option",
            Self::KeyedState(_) => "keyed-state",
            Self::ArrowRow(_) => "arrow-row",
            Self::KeyValue(_) => "key-value",
        }
    }

    pub fn wit_type_def(&self, name: &str, api_version: &ApiVersion) -> Vec<TypeDef> {
        let name = wit_name_case(name);
        let name = map_wit_keyword(&name);

        match self {
            Self::Bool => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::Bool),
            )],
            Self::U8 => vec![TypeDef::new(name, TypeDefKind::Type(wit_encoder::Type::U8))],
            Self::U16 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::U16),
            )],
            Self::U32 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::U32),
            )],
            Self::U64 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::U64),
            )],
            Self::S8 => vec![TypeDef::new(name, TypeDefKind::Type(wit_encoder::Type::S8))],
            Self::S16 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::S16),
            )],
            Self::S32 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::S32),
            )],
            Self::S64 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::S64),
            )],
            Self::Float32 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::F32),
            )],
            Self::Float64 => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::F64),
            )],
            Self::String => vec![TypeDef::new(
                name,
                TypeDefKind::Type(wit_encoder::Type::String),
            )],
            Self::Null => vec![],
            Self::List(inner) => {
                let inner_type = inner.item.wit_type();
                vec![TypeDef::new(
                    name,
                    TypeDefKind::Type(wit_encoder::Type::List(Box::new(inner_type))),
                )]
            }
            Self::Option(inner) => {
                let inner_type = inner.value.wit_type();
                vec![TypeDef::new(
                    name,
                    TypeDefKind::Type(wit_encoder::Type::Option(Box::new(inner_type))),
                )]
            }
            Self::Bytes => {
                vec![TypeDef::new(
                    name,
                    TypeDefKind::Type(wit_encoder::Type::List(Box::new(wit_encoder::Type::U8))),
                )]
            }
            Self::Named(type_ref) => {
                vec![TypeDef::new(name, TypeDefKind::Type(type_ref.wit_type()))]
            }
            Self::KeyValue(sdf_key_value) => {
                vec![TypeDef::new(
                    name,
                    TypeDefKind::Type(sdf_key_value.wit_type()),
                )]
            }
            Self::Object(obj) => {
                vec![TypeDef::new(name, TypeDefKind::Record(obj.wit_record()))]
            }
            Self::Enum(sdf_enum) => {
                vec![TypeDef::new(
                    name,
                    TypeDefKind::Variant(sdf_enum.wit_variant()),
                )]
            }
            Self::KeyedState(sdf_keyed_state) => sdf_keyed_state.wit_type_def(&name, api_version),
            Self::ArrowRow(sdf_arrow_row) => vec![TypeDef::new(
                name,
                TypeDefKind::Record(sdf_arrow_row.wit_record()),
            )],
        }
    }

    pub fn is_native(&self) -> bool {
        matches!(
            self,
            Self::U8
                | Self::U16
                | Self::U32
                | Self::U64
                | Self::S8
                | Self::S16
                | Self::S32
                | Self::S64
                | Self::Float32
                | Self::Float64
                | Self::String
                | Self::Bool
                | Self::Bytes
        )
    }
}

pub(crate) fn ref_type_error(name: &str) -> SdfTypeValidationError {
    SdfTypeValidationError::InvalidRef(name.to_string())
}

pub(crate) fn hashable_primitives_list() -> String {
    HASHABLE_PRIMITIVES
        .iter()
        .map(|ty| ty.ty())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod test {
    use crate::wit::metadata::{
        ArrowColumnKind, EnumField, ObjectField, SdfArrowColumn, SdfArrowRow, SdfEnum,
        SdfKeyedState, SdfKeyedStateValue, SdfList, SdfObject, SdfOption, SdfType, TypeRef,
        SerdeConfig,
    };

    use super::*;

    #[test]
    fn test_validate_type_passes_primitive_types() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Null;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::U8;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::U16;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::U32;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::U64;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::S8;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::S16;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::S32;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::S64;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::Float32;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::Float64;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::String;
        assert_eq!(ty.validate(&map), Ok(()));

        let ty = SdfType::Bool;
        assert_eq!(ty.validate(&map), Ok(()));
    }

    #[test]
    fn test_validate_type_validates_enums() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Enum(SdfEnum {
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
            tagging: None,
        });

        let res = ty
            .validate(&map)
            .expect_err("should error for empty variant name");

        assert_eq!(
            res,
            SdfTypeValidationError::SdfEnum(vec![SdfEnumValidationError::EmptyName])
        );
        assert_eq!(
            res.readable(0),
            r#"Enum type is invalid:
    Enum variant name cannot be empty
"#,
        )
    }

    #[test]
    fn test_validate_accepts_a_valid_enum() {
        let map = SdfTypesMap::default();

        let sdf_enum = SdfEnum {
            tagging: None,
            variants: vec![EnumField {
                name: "my-type".to_string(),
                value: None,
                serde_config: SerdeConfig {
                    serialize: None,
                    deserialize: None,
                },
            }],
        };

        sdf_enum.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_validates_keyed_state_types() {
        let map = SdfTypesMap::default();

        let ty = SdfType::KeyedState(SdfKeyedState {
            key: TypeRef {
                name: "foobar".to_string(),
            },
            value: SdfKeyedStateValue::ArrowRow(SdfArrowRow {
                columns: vec![SdfArrowColumn {
                    name: "number".to_string(),
                    type_: ArrowColumnKind::S32,
                }],
            }),
        });

        let res = ty.validate(&map).expect_err("should error invalid TypeRef");

        assert_eq!(
            res,
            SdfTypeValidationError::SdfKeyedState(vec![
                SdfKeyedStateValidationError::InvalidKeyRef("foobar".to_string())
            ])
        );
        assert_eq!(
            res.readable(0),
            r#"Keyed state type is invalid:
    Referenced key type `foobar` not found in config or imported types
"#,
        )
    }

    #[test]
    fn test_validate_type_accepts_valid_list_declarations() {
        let map = SdfTypesMap::default();

        let ty = SdfType::List(SdfList {
            item: TypeRef {
                name: "u8".to_string(),
            },
        });

        ty.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_type_rejects_invalid_types_in_list_declaration() {
        let map = SdfTypesMap::default();

        let ty = SdfType::List(SdfList {
            item: TypeRef {
                name: "foobar".to_string(),
            },
        });

        let res = ty.validate(&map).expect_err("should error invalid TypeRef");

        assert_eq!(
            res,
            SdfTypeValidationError::InvalidRef("foobar".to_string())
        );
        assert_eq!(
            res.readable(0),
            "Referenced type `foobar` not found in config or imported types\n",
        )
    }

    #[test]
    fn test_validate_type_accepts_valid_option_declarations() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Option(SdfOption {
            value: TypeRef {
                name: "u8".to_string(),
            },
        });

        ty.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_type_rejects_invalid_types_in_option_declaration() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Option(SdfOption {
            value: TypeRef {
                name: "foobar".to_string(),
            },
        });

        let res = ty.validate(&map).expect_err("should error invalid TypeRef");

        assert_eq!(
            res,
            SdfTypeValidationError::InvalidRef("foobar".to_string())
        );
        assert_eq!(
            res.readable(0),
            "Referenced type `foobar` not found in config or imported types\n",
        )
    }

    #[test]
    fn test_validate_type_validates_object() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Object(SdfObject {
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
        });

        let res = ty
            .validate(&map)
            .expect_err("should error empty field name");

        assert_eq!(
            res,
            SdfTypeValidationError::SdfObject(vec![SdfObjectValidationError::EmptyName])
        );
        assert_eq!(
            res.readable(0),
            r#"Object type is invalid:
    Field name cannot be empty
"#,
        )
    }

    #[test]
    fn test_validate_type_accepts_renamed_type() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Named(TypeRef {
            name: "string".to_string(),
        });

        ty.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_type_rejects_renamed_non_existent_type() {
        let map = SdfTypesMap::default();

        let ty = SdfType::Named(TypeRef {
            name: "foobar".to_string(),
        });

        let res = ty.validate(&map).expect_err("should error invalid TypeRef");

        assert_eq!(
            res,
            SdfTypeValidationError::InvalidRef("foobar".to_string())
        );
        assert_eq!(
            res.readable(0),
            "Referenced type `foobar` not found in config or imported types\n",
        )
    }

    #[test]
    fn test_validate_type_validates_arrow_row() {
        let map = SdfTypesMap::default();

        let ty = SdfType::ArrowRow(SdfArrowRow {
            columns: vec![SdfArrowColumn {
                name: "".to_string(),
                type_: ArrowColumnKind::S32,
            }],
        });

        let res = ty
            .validate(&map)
            .expect_err("should error empty arrow-row column");

        assert_eq!(
            res,
            SdfTypeValidationError::SdfArrowRow(vec![SdfArrowRowValidationError::EmptyColumnName])
        );
        assert_eq!(
            res.readable(0),
            r#"Arrow row type is invalid:
    Column name cannot be empty
"#,
        )
    }
}
