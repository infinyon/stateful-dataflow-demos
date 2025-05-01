use croner::Cron;

use crate::{
    util::config_error::{ConfigError, INDENT},
    wit::dataflow::{ScheduleConfig, Schedule as ScheduleWit},
};

impl ScheduleConfig {
    pub fn validate(&self) -> Result<(), ScheduleValidationFailure> {
        match &self.schedule {
            ScheduleWit::Cron(format) => match Cron::new(format).parse() {
                Ok(_) => Ok(()),
                Err(e) => Err(ScheduleValidationFailure {
                    name: self.name.clone(),
                    errors: vec![ScheduleValidationError::InvalidSchedule(e.to_string())],
                }),
            },
        }
    }

    pub fn as_cron(&self) -> Option<Cron> {
        match &self.schedule {
            ScheduleWit::Cron(format) => Cron::new(format).parse().ok(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduleValidationFailure {
    pub name: String,
    pub errors: Vec<ScheduleValidationError>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ScheduleValidationError {
    InvalidSchedule(String),
}

impl ConfigError for ScheduleValidationFailure {
    fn readable(&self, indents: usize) -> String {
        let mut result = format!(
            "{}Schedule `{}` is invalid:\n",
            INDENT.repeat(indents),
            self.name
        );

        for error in &self.errors {
            result.push_str(&error.readable(indents + 1));
        }

        result
    }
}

impl ConfigError for ScheduleValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::InvalidSchedule(error) => {
                format!("{}Failed to parse cron config: {}\n", indent, error)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_validate_invalid_cron_format() {
        let schedule = ScheduleConfig {
            name: "test".to_string(),
            schedule: ScheduleWit::Cron("invalid".to_string()),
        };

        let result = schedule.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.name, "test");
        assert_eq!(err.errors.len(), 1);
        assert_eq!(
            err.errors[0],
            ScheduleValidationError::InvalidSchedule("Invalid pattern: Pattern must consist of five or six fields (minute, hour, day, month, day of week, and optional second).".to_string())
        );
    }
}
