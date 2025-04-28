use crate::{
    wit::states::StateRef,
    util::{validate::SimpleValidate, validation_error::ValidationError},
};

impl SimpleValidate for StateRef {
    fn validate(&self) -> Result<(), ValidationError> {
        let ref_state_explanation = "state reference must be of the form <service>.<state>";

        if self.name.is_empty() && self.ref_service.is_empty() {
            return Err(ValidationError::new(&format!(
                "empty state reference found. {ref_state_explanation}"
            )));
        }

        if self.name.is_empty() {
            return Err(ValidationError::new(&format!(
                "state name missing for state reference. {ref_state_explanation}"
            )));
        }

        if self.ref_service.is_empty() {
            return Err(ValidationError::new(&format!(
                "service name missing for state reference. {ref_state_explanation}"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        wit::states::StateRef,
        util::{validate::SimpleValidate, validation_error::ValidationError},
    };

    #[test]
    fn test_validate_validates_service_is_not_empty() {
        let state = StateRef {
            name: "state".to_string(),
            ref_service: "".to_string(),
        };

        assert_eq!(state.validate(), Err(ValidationError::new("service name missing for state reference. state reference must be of the form <service>.<state>")));
    }

    #[test]
    fn test_validate_validates_state_is_not_empty() {
        let state = StateRef {
            name: "".to_string(),
            ref_service: "my-service".to_string(),
        };

        assert_eq!(state.validate(), Err(ValidationError::new("state name missing for state reference. state reference must be of the form <service>.<state>")));
    }

    #[test]
    fn test_validate_validates_state_and_service_is_not_empty() {
        let state = StateRef {
            name: "".to_string(),
            ref_service: "".to_string(),
        };

        assert_eq!(state.validate(), Err(ValidationError::new("empty state reference found. state reference must be of the form <service>.<state>")));
    }

    #[test]
    fn test_validate_accepts_valid_state_refs() {
        let state = StateRef {
            name: "state".to_string(),
            ref_service: "my-service".to_string(),
        };

        assert_eq!(state.validate(), Ok(()));
    }
}
