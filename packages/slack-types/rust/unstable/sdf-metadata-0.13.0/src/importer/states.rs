use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::wit::{operator::StepState, states::State};

pub fn inject_states(
    service_states: &mut BTreeMap<String, State>,
    step_states: &[StepState],
) -> Result<()> {
    for state in step_states {
        match state {
            StepState::Resolved(s) => {
                let state = State::Typed(s.clone());

                if let Some(old_state_def) = service_states.insert(s.name.clone(), state.clone()) {
                    if old_state_def != state {
                        return Err(anyhow!("state {} is already defined", s.name));
                    }
                }
            }
            StepState::Unresolved(s) => {
                return Err(anyhow!("state {} is not resolved", s.name));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    #[test]
    fn test_inject_states() {
        let mut service_states = std::collections::BTreeMap::new();
        let step_states = vec![crate::wit::operator::StepState::Resolved(
            crate::wit::states::StateTyped {
                name: "state".to_string(),
                type_: crate::wit::metadata::SdfKeyedState {
                    key: crate::wit::metadata::TypeRef {
                        name: "string".to_string(),
                    },
                    value: crate::wit::metadata::SdfKeyedStateValue::Unresolved(
                        crate::wit::metadata::TypeRef {
                            name: "my-state-value".to_string(),
                        },
                    ),
                },
            },
        )];

        assert!(service_states.is_empty());

        let result = crate::importer::states::inject_states(&mut service_states, &step_states);
        assert!(result.is_ok());

        assert_eq!(service_states.len(), 1);
    }
}
