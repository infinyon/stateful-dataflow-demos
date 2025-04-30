pub mod config;

pub use parser::*;

pub mod parser {

    use crate::config;

    pub fn parse(data_pipeline: &str) -> anyhow::Result<config::DataflowDefinitionConfig> {
        let yd = serde_yaml::Deserializer::from_str(data_pipeline);
        let config = serde_path_to_error::deserialize(yd)?;

        Ok(config)
    }
}
