use crate::util::config_error::INDENT;

use super::{config_error::ConfigError, validation_error::ValidationError};

#[derive(Debug, Default, Clone, Eq)]
pub struct ValidationFailure {
    pub errors: Vec<ValidationError>,
}

impl ValidationFailure {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn any(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn push_str(&mut self, msg: &str) {
        self.errors.push(ValidationError::new(msg));
    }

    pub fn push(&mut self, validation_error: &ValidationError) {
        self.errors.push(validation_error.clone());
    }

    pub fn concat(&mut self, other: &Self) {
        self.errors.extend(other.errors.iter().cloned());
    }

    pub fn concat_with_context(&mut self, context: &str, other: &Self) {
        for ValidationError { msg } in other.errors.iter() {
            self.errors
                .push(ValidationError::new(&format!("{} {}", context, msg)));
        }
    }
}

impl ConfigError for ValidationFailure {
    fn readable(&self, indent: usize) -> String {
        self.errors
            .iter()
            .map(|error| format!("{}{}\n", INDENT.repeat(indent), error.msg))
            .collect::<Vec<String>>()
            .join("")
    }
}

impl From<&str> for ValidationFailure {
    fn from(msg: &str) -> Self {
        let mut errors = ValidationFailure::new();

        errors.push_str(msg);

        errors
    }
}

impl PartialEq for ValidationFailure {
    fn eq(&self, other: &Self) -> bool {
        let mut errors = self.errors.clone();
        let mut other_errors = other.errors.clone();

        errors.sort();
        other_errors.sort();

        errors == other_errors
    }
}

impl std::fmt::Display for ValidationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.errors {
            writeln!(f, "{}", error.msg)?;
        }

        Ok(())
    }
}
