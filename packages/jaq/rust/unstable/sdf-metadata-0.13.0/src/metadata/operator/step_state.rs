use anyhow::Result;

use crate::wit::{operator::StepState, states::StateTyped};

impl StepState {
    pub fn is_resolved(&self) -> bool {
        matches!(self, Self::Resolved(_))
    }
    pub fn resolve(&mut self, states: &[StateTyped]) -> Result<()> {
        match self {
            Self::Resolved(_) => return Ok(()),
            Self::Unresolved(imported_state) => {
                let state = states
                    .iter()
                    .find(|state| state.name == *imported_state.name);
                if let Some(state) = state {
                    *self = Self::Resolved(state.clone());
                } else {
                    return Err(anyhow::anyhow!(
                        "Could not resolve state: {}",
                        imported_state.name
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Resolved(state) => &state.name,
            Self::Unresolved(imported_state) => &imported_state.name,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::wit::{
        metadata::{SdfKeyedState, SdfKeyedStateValue, TypeRef},
        operator::{StateImport, StepState},
        states::StateTyped,
    };

    #[test]
    fn test_resolve_step_state() {
        let mut step_state = StepState::Unresolved(StateImport {
            name: "my-imported-state".to_string(),
        });

        let states = vec![StateTyped {
            name: "my-imported-state".to_string(),
            type_: SdfKeyedState {
                key: TypeRef {
                    name: "string".to_string(),
                },
                value: SdfKeyedStateValue::U32,
            },
        }];

        step_state.resolve(&states).unwrap();

        assert!(step_state.is_resolved());

        if let StepState::Resolved(state) = step_state {
            assert_eq!(state.name, "my-imported-state");
            assert_eq!(state.type_.key.name, "string");
            assert_eq!(state.type_.value, SdfKeyedStateValue::U32);
        } else {
            panic!("Expected StepState::Resolved");
        }
    }
}
