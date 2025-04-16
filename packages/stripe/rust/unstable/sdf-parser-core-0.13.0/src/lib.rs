pub mod config;
pub use parser::*;

pub mod parser {

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(untagged)]
    pub enum MaybeValid<U> {
        Valid(U),
        Invalid(serde_yaml::Value),
    }

    impl<U> MaybeValid<U> {
        pub fn is_invalid(&self) -> bool {
            matches!(self, Self::Invalid(_))
        }

        pub fn valid_data(&self) -> Option<&U> {
            match self {
                Self::Valid(data) => Some(data),
                Self::Invalid(_) => None,
            }
        }
    }
}
