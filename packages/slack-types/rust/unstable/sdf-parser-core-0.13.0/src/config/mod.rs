pub mod transform;
pub mod types;
pub mod import;
pub mod dev;
pub mod utils;

use sdf_common::LATEST_STABLE_DATAFLOW;

pub const SERVICE_DEFINITION_CONFIG_STABLE_VERSION: &str = LATEST_STABLE_DATAFLOW;
pub const VERSION_NOT_SUPPORTED_ERROR: &str = "ApiVersion not supported, try upgrading to 0.5.0";

pub use wrapper::*;

mod wrapper {

    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;

    use self::types::MetadataType;

    /// Common metadata
    #[derive(Serialize, Deserialize, Debug, Clone, Default, JsonSchema)]
    pub struct Metadata {
        pub name: String,
        pub version: String,
        pub namespace: String,
    }

    #[derive(Serialize, Deserialize, Debug, Default, Clone, JsonSchema)]
    pub struct DefaultConfigs {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub consumer: Option<ConsumerConfigWrapper>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub converter: Option<SerdeConverter>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub producer: Option<ProducerConfigWrapper>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    #[serde(rename_all = "kebab-case")]
    pub struct TopicWrapper {
        pub name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub consumer: Option<ConsumerConfigWrapper>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub producer: Option<ProducerConfigWrapper>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            default,
            alias = "remote_cluster_profile"
        )]
        pub remote_cluster_profile: Option<String>,
        pub schema: SchemaWrapper,
    }

    impl TopicWrapper {
        pub fn ty(&self) -> &str {
            self.schema.value.ty.ty()
        }

        pub fn with_defaults(&self, topic_id: &str, defaults: &DefaultConfigs) -> Self {
            Self {
                name: self.name.clone().or_else(|| Some(topic_id.into())),
                consumer: self.consumer.clone().or(defaults.consumer.clone()),
                producer: self.producer.clone().or(defaults.producer.clone()),
                remote_cluster_profile: self.remote_cluster_profile.clone(),
                schema: SchemaWrapper {
                    key: self
                        .schema
                        .clone()
                        .key
                        .map(|inner| inner.with_defaults(defaults)),
                    value: self.schema.value.with_defaults(defaults),
                },
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    pub struct SchemaWrapper {
        #[serde(default)]
        pub key: Option<SerdeConfig>,
        pub value: SerdeConfig,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    pub struct SerdeConfig {
        #[serde(flatten)]
        pub ty: MetadataType,
        pub converter: Option<SerdeConverter>,
    }

    impl SerdeConfig {
        pub fn with_defaults(&self, defaults: &DefaultConfigs) -> Self {
            Self {
                ty: self.ty.clone(),
                converter: self.converter.clone().or(defaults.converter.clone()),
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
    #[serde(rename_all = "kebab-case")]
    pub enum SerdeConverter {
        Json,
        Raw,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    pub struct ConsumerConfigWrapper {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub default_starting_offset: Option<OffsetWrapper>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub max_bytes: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub isolation: Option<Isolation>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
    #[serde(tag = "position", content = "value")]
    pub enum OffsetWrapper {
        Offset(i64),
        Beginning(u32),
        End(u32),
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    pub enum Isolation {
        ReadUncommitted,
        ReadCommitted,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    #[serde(rename_all = "kebab-case")]
    pub enum Compression {
        Gzip,
        Snappy,
        Lz4,
        Zstd,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
    pub struct ProducerConfigWrapper {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub batch_size: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub linger_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub compression: Option<Compression>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub timeout_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub isolation: Option<Isolation>,
    }
}
