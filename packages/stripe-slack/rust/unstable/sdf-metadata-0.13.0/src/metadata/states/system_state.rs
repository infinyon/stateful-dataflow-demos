use crate::wit::states::SystemState;

impl SystemState {
    pub fn validate(&self) -> Result<(), crate::util::validation_error::ValidationError> {
        if self.name.is_empty() && self.system.is_empty() {
            return Err(crate::util::validation_error::ValidationError::new(
                "empty system state found. state name and system cannot be empty",
            ));
        }

        if self.name.is_empty() {
            return Err(crate::util::validation_error::ValidationError::new(
                "Name must be specified for system state",
            ));
        }

        if self.system.is_empty() {
            return Err(crate::util::validation_error::ValidationError::new(
                "System must be specified for system state `name`",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{wit::states::SystemState, util::validation_error::ValidationError};

    #[test]
    fn test_validate_rejects_empty_system() {
        let state = SystemState {
            name: "my-state".to_string(),
            system: "".to_string(),
        };

        assert_eq!(
            state.validate(),
            Err(ValidationError::new(
                "System must be specified for system state `name`"
            ))
        );
    }

    #[test]
    fn test_validate_rejects_empty_name() {
        let state = SystemState {
            name: "".to_string(),
            system: "my-system".to_string(),
        };

        assert_eq!(
            state.validate(),
            Err(ValidationError::new(
                "Name must be specified for system state"
            ))
        );
    }

    #[test]
    fn test_validate_rejects_empty_system_state() {
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

    #[test]
    fn test_validate_accepts_valid_system_state() {
        let state = SystemState {
            name: "my-state".to_string(),
            system: "my-system".to_string(),
        };

        assert_eq!(state.validate(), Ok(()));
    }
}
