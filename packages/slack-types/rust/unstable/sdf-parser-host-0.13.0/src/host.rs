use anyhow::{Context, Result};
use sdf_metadata::wit::dataflow::DataflowDefinition;
use tracing::debug;

use sdf_metadata::wit::package_interface::PackageDefinition;

use crate::bindings::sdf::parser::yaml_parser::Host as ParserHost;
/// Host based parser
pub struct HostParser {}

impl Default for HostParser {
    fn default() -> Self {
        Self::new()
    }
}

impl HostParser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn parse_dataflow(&mut self, config_string: &str) -> Result<DataflowDefinition> {
        debug!("parsing dataflow");

        let config = sdf_parser_df::parse(config_string)?;
        debug!("parsed dataflow");
        config
            .try_into()
            .with_context(|| "failed to convert dataflow into WIT dataflow")
    }

    pub fn parse_package(&mut self, config_string: &str) -> Result<PackageDefinition> {
        debug!("parsing package");

        let config = sdf_parser_package::parse_package(config_string)?;
        debug!("parsed dataflow");
        config
            .try_into()
            .with_context(|| "failed to convert package into WIT package")
    }
}

impl ParserHost for HostParser {
    fn parse_dataflow(&mut self, config_string: String) -> Result<DataflowDefinition, String> {
        self.parse_dataflow(&config_string)
            .map_err(|err| err.to_string())
    }

    fn parse_package(&mut self, config_string: String) -> Result<PackageDefinition, String> {
        self.parse_package(&config_string)
            .map_err(|err| err.to_string())
    }
}
