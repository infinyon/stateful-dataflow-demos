use crate::{
    wit::states::State,
    util::{validate::SimpleValidate, validation_error::ValidationError},
};

impl State {
    pub fn name(&self) -> &str {
        match self {
            State::Typed(state_typed) => &state_typed.name,
            State::Reference(ref_state) => &ref_state.name,
            State::System(system_state) => &system_state.name,
        }
    }

    pub fn is_owned(&self) -> bool {
        matches!(self, State::Typed(_))
    }
}

impl SimpleValidate for State {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            State::Typed(state_typed) => state_typed.validate(),
            State::Reference(ref_state) => ref_state.validate(),
            State::System(system_state) => system_state.validate(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        wit::{
            io::TypeRef,
            metadata::{SdfKeyedState, SdfKeyedStateValue},
            operator::StateTyped,
            states::{StateRef, SystemState},
        },
        util::{validate::SimpleValidate, validation_error::ValidationError},
    };

    #[test]
    fn test_validate_asserts_typed_state_is_resolved() {
        let state = StateTyped {
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

        assert_eq!(state.validate(), Err(ValidationError::new("Internal Error: typed state value should be resolved before validation. Please contact support")));
    }

    #[test]
    fn test_validate_validates_ref_state_values_are_not_empty() {
        let state = StateRef {
            name: "state".to_string(),
            ref_service: "".to_string(),
        };

        assert_eq!(state.validate(), Err(ValidationError::new("service name missing for state reference. state reference must be of the form <service>.<state>")));
    }

    #[test]
    fn test_validate_validates_system_state_values_are_not_empty() {
        let state = SystemState {
            name: "".to_string(),
            system: "".to_string(),
        };

        assert_eq!(
            state.validate(),
            Err(ValidationError::new(
                "empty system state found. state name and system cannot be empty"
            ))
        );
    }
}
