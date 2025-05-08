#[cfg(test)]
mod tests;

pub mod utils;

use sdf_common::LATEST_STABLE_DATAFLOW;

pub const SERVICE_DEFINITION_CONFIG_STABLE_VERSION: &str = LATEST_STABLE_DATAFLOW;
pub const VERSION_NOT_SUPPORTED_ERROR: &str = "ApiVersion not supported, try upgrading to 0.5.0";

pub use wrapper::*;

mod wrapper {

    use std::{
        collections::BTreeMap,
        ops::{Deref, DerefMut},
    };

    use anyhow::{anyhow, Result};
    use sdf_parser_package::pkg::PackageWrapperV0_5_0;
    use serde::{Deserialize, Serialize};

    use sdf_parser_core::{
        config::{
            dev::DevConfig,
            import::PackageImport,
            types::{MetadataTypesMap, MetadataTypesMapWrapper},
            DefaultConfigs, Metadata, TopicWrapper,
        },
        MaybeValid,
    };

    use super::*;

    use sdf_parser_core::config::transform::{
        TransformOperator, PartitionOperatorWrapper, StateWrapper, TransformsWrapperV0_5_0,
        WindowOperatorWrapper,
    };

    /// Current dataflow definition config, this should be
    pub type CurrentDataflowDefinitionConfig = DataflowDefinitionWrapperV0_5_0;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(tag = "apiVersion")]
    pub enum DataflowDefinitionConfig {
        #[serde(rename = "0.5.0")]
        V0_5_0(CurrentDataflowDefinitionConfig),
        #[serde(rename = "0.6.0")]
        V0_6_0(CurrentDataflowDefinitionConfig),
        #[serde(rename = "0.1.0", alias = "0.2.0", alias = "0.3.0", alias = "0.4.0")]
        Unsupported(DataflowDefinitionUnsupportedVersion),
    }

    impl Deref for DataflowDefinitionConfig {
        type Target = CurrentDataflowDefinitionConfig;

        fn deref(&self) -> &Self::Target {
            match self {
                Self::V0_5_0(inner) => inner,
                Self::V0_6_0(inner) => inner,
                Self::Unsupported(_) => unreachable!("{VERSION_NOT_SUPPORTED_ERROR}"),
            }
        }
    }

    impl DerefMut for DataflowDefinitionConfig {
        fn deref_mut(&mut self) -> &mut Self::Target {
            match self {
                Self::V0_5_0(inner) => inner,
                Self::V0_6_0(inner) => inner,
                Self::Unsupported(_) => unreachable!("{VERSION_NOT_SUPPORTED_ERROR}"),
            }
        }
    }

    impl CurrentDataflowDefinitionConfig {
        pub fn name(&self) -> &str {
            &self.meta.name
        }

        pub fn version(&self) -> &str {
            &self.meta.version
        }

        pub fn namespace(&self) -> &str {
            &self.meta.namespace
        }

        pub fn metadata(&self) -> &Metadata {
            &self.meta
        }

        pub fn imports(&self) -> &Vec<PackageImport> {
            &self.imports
        }

        pub fn services(&self) -> Result<Vec<(&String, OperationsWrapperV0_5_0)>> {
            self.services
                .iter()
                .map(|(a, b)| match b.valid_data() {
                    Some(b) => Ok((a, b.to_owned())),
                    None => Err(anyhow!("Invalid service definition")),
                })
                .collect::<Result<_>>()
        }

        pub fn dev(&self) -> Option<&DevConfig> {
            self.dev.as_ref()
        }
    }

    pub type DataflowMetadata = Metadata;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct DataflowDefinitionUnsupportedVersion {
        pub meta: DataflowMetadata,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct DataflowDefinitionWrapperV0_5_0 {
        pub meta: DataflowMetadata,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub imports: Vec<PackageImport>,
        #[serde(with = "serde_with::rust::maps_duplicate_key_is_error")]
        pub services: BTreeMap<String, MaybeValid<OperationsWrapperV0_5_0>>,
        #[serde(default)]
        pub types: MetadataTypesMapWrapper,
        #[serde(
            skip_serializing_if = "BTreeMap::is_empty",
            with = "serde_with::rust::maps_duplicate_key_is_error",
            default
        )]
        pub topics: BTreeMap<String, TopicWrapper>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub config: Option<DefaultConfigs>,
        pub dev: Option<DevConfig>,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub packages: Vec<PackageWrapperV0_5_0>,
        #[serde(
            skip_serializing_if = "BTreeMap::is_empty",
            with = "serde_with::rust::maps_duplicate_key_is_error",
            default
        )]
        pub schedule: BTreeMap<String, ScheduleWrapper>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "kebab-case")]
    pub enum ScheduleWrapper {
        Cron(String),
    }

    impl DataflowDefinitionWrapperV0_5_0 {
        pub fn types(&self) -> Result<MetadataTypesMap> {
            let mut types: MetadataTypesMap = self.types.clone().try_into()?;

            let services = self.services()?;

            for (name, service) in services.iter() {
                for (state_name, state) in &service.states {
                    if state.is_invalid() {
                        return Err(anyhow!(
                            "Invalid state definition for service {}: {}",
                            name,
                            state_name
                        ));
                    }
                }
            }

            services.iter().fold(&mut types, |acc, (_, op)| {
                for (name, state) in &op.states {
                    let Some(state) = state.valid_data() else {
                        unreachable!();
                    };
                    if let Some(state_type) = state.inner_type() {
                        acc.insert(name.into(), state_type.clone());
                    }
                }
                acc
            });
            Ok(types)
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct OperationsWrapperV0_5_0 {
        pub sources: Vec<IoRefWrapper>,
        #[serde(default = "IoRefWrapper::default_vec")]
        pub sinks: Vec<IoRefWrapper>,
        #[serde(default)]
        pub transforms: TransformsWrapperV0_5_0,
        #[serde(flatten)]
        pub post_transforms: Option<PostTransforms>,
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub states: BTreeMap<String, MaybeValid<StateWrapper>>,
    }

    pub type PostTransforms = MaybeValid<PostTransformsInner>;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "kebab-case")]
    pub enum PostTransformsInner {
        Window(WindowOperatorWrapper),
        Partition(PartitionOperatorWrapper),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(tag = "type", rename_all = "kebab-case")]
    pub enum IoRefWrapper {
        Topic(IoConfigRef),
        NoTarget,
        Schedule(IoConfigRef),
    }

    impl Default for IoRefWrapper {
        fn default() -> Self {
            Self::NoTarget
        }
    }

    impl IoRefWrapper {
        fn default_vec() -> Vec<IoRefWrapper> {
            vec![Self::default()]
        }
        pub fn id(&self) -> Result<&str> {
            match self {
                Self::Topic(topic) => Ok(&topic.id),
                Self::NoTarget => Err(anyhow!("No target specified")),
                Self::Schedule(schedule) => Ok(&schedule.id),
            }
        }

        pub fn transforms(&self) -> Vec<TransformOperator> {
            match self {
                Self::Topic(topic) => topic.transforms.clone(),
                Self::NoTarget => vec![],
                Self::Schedule(schedule) => schedule.transforms.clone(),
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct IoConfigRef {
        pub id: String,
        #[serde(default)]
        pub transforms: Vec<TransformOperator>,
    }
}
