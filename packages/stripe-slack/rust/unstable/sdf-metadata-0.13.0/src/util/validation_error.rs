use crate::util::config_error::INDENT;

use super::config_error::ConfigError;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ValidationError {
    pub msg: String,
}

impl ValidationError {
    pub fn new(msg: &str) -> Self {
        Self {
            msg: msg.to_string(),
        }
    }

    pub fn add_context(&self, context: &str) -> Self {
        Self {
            msg: format!("{} {}", context, self.msg),
        }
    }
}

impl ConfigError for ValidationError {
    fn readable(&self, indent: usize) -> String {
        format!("{}{}\n", INDENT.repeat(indent), self.msg)
    }
}
