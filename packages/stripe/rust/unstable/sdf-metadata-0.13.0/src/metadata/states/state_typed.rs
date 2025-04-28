use anyhow::{anyhow, Result};

use crate::{
    wit::{
        metadata::{SdfKeyedStateValue, SdfType},
        states::StateTyped,
    },
    util::{
        sdf_types_map::SdfTypesMap, validate::SimpleValidate, validation_error::ValidationError,
    },
};

impl StateTyped {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn resolve(&mut self, types: &SdfTypesMap) -> Result<()> {
        if let SdfKeyedStateValue::Unresolved(ref_type) = &self.type_.value {
            if let Some(ty) = types.get(&ref_type.name) {
                match &ty.0 {
                    SdfType::U32 => {
                        self.type_.value = SdfKeyedStateValue::U32;
                    }
                    SdfType::ArrowRow(row) => {
                        self.type_.value = SdfKeyedStateValue::ArrowRow(row.clone());
                    }
                    _ => {
                        return Err(anyhow!("invalid type for keyed state value"));
                    }
                }
            }
        }

        Ok(())
    }
}

impl SimpleValidate for StateTyped {
    fn validate(&self) -> Result<(), ValidationError> {
        match &self.type_.value {
            SdfKeyedStateValue::Unresolved(_) => {
                Err(ValidationError::new(
                    "Internal Error: typed state value should be resolved before validation. Please contact support",
                ))
            }
            SdfKeyedStateValue::U32 => Ok(()),
            SdfKeyedStateValue::ArrowRow(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        wit::{
            metadata::{SdfArrowRow, SdfKeyedState, SdfKeyedStateValue, TypeRef},
            states::StateTyped,
        },
        util::{sdf_types_map::SdfTypesMap, validate::SimpleValidate},
    };

    #[test]
    fn test_resolve_state_typed_u32() {
        let mut state = StateTyped {
            name: "state".to_string(),
            type_: SdfKeyedState {
                key: TypeRef {
                    name: "string".to_string(),
                },
                value: SdfKeyedStateValue::Unresolved(TypeRef {
                    name: "my-state-value".to_string(),
                }),
            },
        };

        let mut types = SdfTypesMap::default();
        types.insert_local(
            "my-state-value".to_string(),
            crate::wit::metadata::SdfType::U32,
        );

        state.resolve(&types).unwrap();

        assert_eq!(
            state.type_.value,
            SdfKeyedStateValue::U32,
            "state type should be resolved to U32"
        );
    }

    #[test]
    fn test_resolve_state_typed_row() {
        let mut state = StateTyped {
            name: "state".to_string(),
            type_: SdfKeyedState {
                key: TypeRef {
                    name: "string".to_string(),
                },
                value: SdfKeyedStateValue::Unresolved(TypeRef {
                    name: "my-state-value".to_string(),
                }),
            },
        };

        let mut types = SdfTypesMap::default();
        types.insert_local(
            "my-state-value".to_string(),
            crate::wit::metadata::SdfType::ArrowRow(SdfArrowRow { columns: vec![] }),
        );

        state.resolve(&types).unwrap();

        assert!(
            matches!(state.type_.value, SdfKeyedStateValue::ArrowRow(_)),
            "state type should be resolved to arrow row"
        );
    }

    #[test]
    fn test_validate_accepts_valid_type_states() {
        let state = StateTyped {
            name: "state".to_string(),
            type_: SdfKeyedState {
                key: TypeRef {
                    name: "string".to_string(),
                },
                value: SdfKeyedStateValue::U32,
            },
        };

        assert!(state.validate().is_ok());
    }
}
