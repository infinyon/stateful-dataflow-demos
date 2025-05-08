pub mod config;
pub use parser::*;

pub mod parser {

    use schemars::{schema::Schema, JsonSchema};
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

    // we implement this way to avoid json schema generated a schema for the invalid case
    impl<U: JsonSchema> JsonSchema for MaybeValid<U> {
        fn schema_name() -> String {
            U::schema_name()
        }

        fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> Schema {
            U::json_schema(generator)
        }
    }
}
