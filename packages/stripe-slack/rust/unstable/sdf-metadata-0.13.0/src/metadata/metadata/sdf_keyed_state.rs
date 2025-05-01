use wit_encoder::{Tuple, Type, TypeDef, TypeDefKind};

use sdf_common::version::ApiVersion;

use crate::{
    util::{
        config_error::{ConfigError, INDENT},
        sdf_types_map::SdfTypesMap,
    },
    wit::metadata::{SdfKeyedState, SdfKeyedStateValue},
};

use super::{
    sdf_arrow_row::SdfArrowRowValidationError, sdf_object::SdfObjectValidationError,
    sdf_type::SdfTypeValidationError,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SdfKeyedStateValidationError {
    InvalidKeyRef(String),
    InvalidValueRef(String),
    SdfArrowRow(Vec<SdfArrowRowValidationError>),
    SdfObject(Vec<SdfObjectValidationError>),
}

impl From<Vec<SdfKeyedStateValidationError>> for SdfTypeValidationError {
    fn from(errs: Vec<SdfKeyedStateValidationError>) -> Self {
        Self::SdfKeyedState(errs)
    }
}

impl ConfigError for SdfKeyedStateValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::InvalidKeyRef(name) => {
                format!(
                    "{}Referenced key type `{}` not found in config or imported types\n",
                    indent, name
                )
            }
            Self::InvalidValueRef(name) => {
                format!(
                    "{}Referenced type `{}` not found in config or imported types\n",
                    indent, name
                )
            }
            Self::SdfArrowRow(errors) => {
                let mut result = format!("{}Arrow row value is invalid\n", indent);

                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
            Self::SdfObject(errors) => {
                let mut result = String::new();
                for error in errors {
                    result.push_str(&error.readable(indents + 1));
                }
                result
            }
        }
    }
}

impl SdfKeyedState {
    pub fn validate(&self, map: &SdfTypesMap) -> Result<(), Vec<SdfKeyedStateValidationError>> {
        let mut errors = vec![];

        if !map.contains_key(&self.key.name) {
            errors.push(SdfKeyedStateValidationError::InvalidKeyRef(
                self.key.name.clone(),
            ));
        }

        match &self.value {
            SdfKeyedStateValue::ArrowRow(sdf_arrow_row) => {
                if let Err(row_errors) = sdf_arrow_row.validate() {
                    errors.push(SdfKeyedStateValidationError::SdfArrowRow(row_errors));
                }
            }
            SdfKeyedStateValue::Unresolved(type_ref) => {
                if !map.contains_key(&type_ref.name) {
                    errors.push(SdfKeyedStateValidationError::InvalidValueRef(
                        type_ref.name.clone(),
                    ));
                }
            }
            SdfKeyedStateValue::U32 => {}
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn wit_type_def(&self, name: &str, api_version: &ApiVersion) -> Vec<TypeDef> {
        let value_name = format!("{}-item-value", name);
        let wit_value = self.value.wit_type_def(&value_name);

        let state_ty = match &self.value {
            SdfKeyedStateValue::U32 => TypeDef::new(
                name.to_owned(),
                TypeDefKind::Type(wit_encoder::Type::List(Box::new(wit_encoder::Type::Named(
                    format!("{name}-item").into(),
                )))),
            ),
            SdfKeyedStateValue::ArrowRow(_) => {
                let table_ty = api_version.table_wit_type().to_string();

                TypeDef::new(
                    name.to_owned(),
                    TypeDefKind::Type(wit_encoder::Type::Named(table_ty.into())),
                )
            }
            SdfKeyedStateValue::Unresolved(unresolved) => {
                TypeDef::new(name.to_owned(), TypeDefKind::Type(unresolved.wit_type()))
            }
        };

        let state_item_ty = state_item(
            &format!("{name}-item"),
            &self.key.wit_type(),
            &Type::Named(value_name.into()),
        );

        vec![wit_value, state_ty, state_item_ty]
    }
}

fn state_item(name: &str, key_type: &Type, value_type: &Type) -> TypeDef {
    let mut tuple = Tuple::empty();
    tuple.type_(key_type.to_owned());
    tuple.type_(value_type.to_owned());

    TypeDef::new(
        name.to_owned(),
        TypeDefKind::Type(wit_encoder::Type::Tuple(tuple)),
    )
}
#[cfg(test)]
mod test {
    use crate::wit::metadata::{
        ArrowColumnKind, SdfArrowColumn, SdfArrowRow, SdfKeyedState, SdfKeyedStateValue, TypeRef,
    };
    use super::*;

    #[test]
    fn test_validate_accepts_valid_keyed_state_definitions() {
        let map = SdfTypesMap::default();

        let keyed_state = SdfKeyedState {
            key: TypeRef {
                name: "string".to_string(),
            },
            value: SdfKeyedStateValue::ArrowRow(SdfArrowRow {
                columns: vec![SdfArrowColumn {
                    name: "number".to_string(),
                    type_: ArrowColumnKind::S32,
                }],
            }),
        };

        keyed_state.validate(&map).expect("should validate");
    }

    #[test]
    fn test_validate_rejects_keyed_state_definitions_with_invalid_keys() {
        let map = SdfTypesMap::default();

        let keyed_state = SdfKeyedState {
            key: TypeRef {
                name: "foobar".to_string(),
            },
            value: SdfKeyedStateValue::ArrowRow(SdfArrowRow {
                columns: vec![SdfArrowColumn {
                    name: "number".to_string(),
                    type_: ArrowColumnKind::S32,
                }],
            }),
        };

        let res = keyed_state
            .validate(&map)
            .expect_err("should error invalid TypeRef");

        assert!(res.contains(&SdfKeyedStateValidationError::InvalidKeyRef(
            "foobar".to_string()
        )));
        assert_eq!(
            res[0].readable(0),
            "Referenced key type `foobar` not found in config or imported types\n"
        )
    }

    #[test]
    fn test_validate_rejects_keyed_state_definitions_with_missing_arrow_column_name() {
        let map = SdfTypesMap::default();

        let keyed_state = SdfKeyedState {
            key: TypeRef {
                name: "string".to_string(),
            },
            value: SdfKeyedStateValue::ArrowRow(SdfArrowRow {
                columns: vec![SdfArrowColumn {
                    name: "".to_string(),
                    type_: ArrowColumnKind::S32,
                }],
            }),
        };

        let res = keyed_state
            .validate(&map)
            .expect_err("should error for empty column name");

        assert!(
            res.contains(&SdfKeyedStateValidationError::SdfArrowRow(vec![
                SdfArrowRowValidationError::EmptyColumnName
            ]))
        );
        assert_eq!(
            res[0].readable(0),
            r#"Arrow row value is invalid
    Column name cannot be empty
"#,
        )
    }

    #[test]
    fn test_validate_rejects_keyed_state_definitions_invalid_value_types() {
        let map = SdfTypesMap::default();

        let keyed_state = SdfKeyedState {
            key: TypeRef {
                name: "u8".to_string(),
            },
            value: SdfKeyedStateValue::Unresolved(TypeRef {
                name: "foobar".to_string(),
            }),
        };

        let res = keyed_state
            .validate(&map)
            .expect_err("should error invalid TypeRef");

        assert!(res.contains(&SdfKeyedStateValidationError::InvalidValueRef(
            "foobar".to_string()
        )));
        assert_eq!(
            res[0].readable(0),
            "Referenced type `foobar` not found in config or imported types\n",
        )
    }
}
