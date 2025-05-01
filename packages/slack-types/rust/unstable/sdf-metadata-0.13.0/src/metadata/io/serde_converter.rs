use std::fmt::Display;

use crate::wit::package_interface::SerdeConverter;

impl Display for SerdeConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerdeConverter::Json => write!(f, "json"),
            SerdeConverter::Raw => write!(f, "raw"),
        }
    }
}
