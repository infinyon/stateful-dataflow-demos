use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use sdf_common::constants::{DATAFLOW_STABLE_VERSION, DATAFLOW_V5_VERSION, DATAFLOW_V6_VERSION};
use sdf_common::version::ApiVersion;
use sdf_parser_df::config::{
    DataflowDefinitionConfig, DataflowDefinitionWrapperV0_5_0, DataflowMetadata, IoRefWrapper,
    OperationsWrapperV0_5_0, ScheduleWrapper,
};

mod types;
mod transform;
mod import;
pub mod pkg_into_wit;

pub(crate) use crate::wit as config;
use crate::into_wit::types::IntoBinding;

use sdf_parser_core::{
    config::{
        types::MetadataTypesMap, Compression, ConsumerConfigWrapper, DefaultConfigs, Isolation,
        OffsetWrapper, ProducerConfigWrapper, SchemaWrapper, SerdeConfig, SerdeConverter,
        TopicWrapper, VERSION_NOT_SUPPORTED_ERROR,
    },
    MaybeValid,
};

use crate::into_wit::config::{
    dataflow::{
        DataflowDefinition as DataflowDefinitionWit, Header as HeaderWit,
        DevConfig as DevConfigWit, IoRef as IoRefWit, IoType as IoTypeWit,
        Operations as OperationsWit, PackageImport as PackageImportWit, State as StateWit,
        Topic as TopicWit, Transforms as TransformsWit, Schedule as ScheduleWit,
        ScheduleConfig as ScheduleConfigWit,
    },
    package_interface::SerdeConverter as SerdeConverterWit,
    io::{
        Compression as CompressionWit, ConsumerConfig as ConsumerConfigWit,
        Isolation as IsolationWit, Offset as OffsetWit, ProducerConfig as ProducerConfigWit,
        SchemaSerDe as SchemaSerDeWit, TopicSchema as TopicSchemaWit, TypeRef as TypeRefWit,
    },
};

impl TryFrom<DataflowDefinitionConfig> for DataflowDefinitionWit {
    type Error = anyhow::Error;
    fn try_from(config: DataflowDefinitionConfig) -> Result<Self> {
        match config {
            DataflowDefinitionConfig::Unsupported(_) => {
                Err(anyhow!("{VERSION_NOT_SUPPORTED_ERROR}"))
            }
            DataflowDefinitionConfig::V0_5_0(wrapper) => {
                let mut wit_config: DataflowDefinitionWit =
                    wrapper.try_into_binding(&ApiVersion::V5)?;
                wit_config.api_version = DATAFLOW_V5_VERSION.to_string();
                Ok(wit_config)
            }

            DataflowDefinitionConfig::V0_6_0(wrapper) => {
                let mut wit_config: DataflowDefinitionWit =
                    wrapper.try_into_binding(&ApiVersion::V6)?;

                wit_config.api_version = DATAFLOW_V6_VERSION.to_string();
                Ok(wit_config)
            }
        }
    }
}

pub(crate) trait TryIntoBinding {
    type Target;
    fn try_into_binding(self, api_version: &ApiVersion) -> Result<Self::Target>;
}

impl TryIntoBinding for DataflowDefinitionWrapperV0_5_0 {
    type Target = DataflowDefinitionWit;
    fn try_into_binding(self, api_version: &ApiVersion) -> Result<Self::Target> {
        let defaults = self.config.clone().unwrap_or_default();
        let types = self.types()?;
        let df_types = types
            .iter()
            .map(|(name, ty)| ty.to_owned().into_bindings(name, &types, api_version))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // check that types are not repeated
        let mut df_types_deduplicated = BTreeMap::new();
        for ty in &df_types {
            if let Some(prev_ty) = df_types_deduplicated.insert(&ty.name, ty.to_owned()) {
                if prev_ty != *ty {
                    return Err(anyhow!(
                        "Type {} is defined multiple times with different definitions",
                        ty.name
                    ));
                }
            }
        }

        let df_types = df_types_deduplicated.into_values().collect::<Vec<_>>();
        let schedule_config = self
            .schedule
            .into_iter()
            .map(|(key, v)| ScheduleConfigWit {
                name: key,
                schedule: v.into(),
            })
            .collect::<Vec<_>>();

        let schedule = if schedule_config.is_empty() {
            None
        } else {
            Some(schedule_config)
        };
        Ok(DataflowDefinitionWit {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: self.meta.into(),
            imports: self
                .imports
                .into_iter()
                .map(|import| import.into())
                .collect::<Vec<PackageImportWit>>(),
            types: df_types,
            services: self
                .services
                .into_iter()
                .map(|(name, op)| {
                    op.valid_data()
                        .ok_or_else(|| anyhow!("Invalid service definition for {name}"))
                        .and_then(|op| op.clone().try_into_bindings(name, &types))
                })
                .collect::<Result<_>>()?,
            topics: self
                .topics
                .into_iter()
                .map(|(key, v)| (key.clone(), v.into_bindings(&key, &defaults)))
                .collect(),
            dev: self.dev.map(|d| DevConfigWit {
                converter: d.converter.map(|c| c.into()),
                imports: d.imports.into_iter().map(|i| i.into()).collect(),
                topics: d
                    .topics
                    .into_iter()
                    .map(|(key, v)| (key.clone(), v.into_bindings(&key, &defaults)))
                    .collect(),
            }),
            packages: vec![],
            schedule,
        })
    }
}
impl From<ScheduleWrapper> for ScheduleWit {
    fn from(wrapper: ScheduleWrapper) -> Self {
        match wrapper {
            ScheduleWrapper::Cron(cron) => Self::Cron(cron),
        }
    }
}

impl From<DataflowMetadata> for HeaderWit {
    fn from(meta: DataflowMetadata) -> Self {
        Self {
            name: meta.name,
            version: meta.version,
            namespace: meta.namespace,
        }
    }
}

trait TryIntoBindings {
    fn try_into_bindings(self, name: String, types: &MetadataTypesMap) -> Result<OperationsWit>;
}

impl TryIntoBindings for OperationsWrapperV0_5_0 {
    fn try_into_bindings(
        self,
        service_name: String,
        types: &MetadataTypesMap,
    ) -> Result<OperationsWit> {
        let post_transforms = match self.post_transforms {
            Some(MaybeValid::Valid(inner)) => Some(inner.try_into()?),
            Some(MaybeValid::Invalid(invalid)) => {
                if invalid == serde_yaml::Value::Mapping(Default::default()) {
                    None
                } else {
                    return Err(anyhow!("Invalid post_transforms: {:?}", invalid));
                }
            }
            None => None,
        };
        Ok(OperationsWit {
            name: service_name.clone(),
            sources: self
                .sources
                .into_iter()
                .map(|s| s.try_into())
                .collect::<Result<_>>()?,
            sinks: self
                .sinks
                .into_iter()
                .map(|s| s.try_into())
                .collect::<Result<_>>()?,
            states: self
                .states
                .into_iter()
                .map(|(s_name, s)| {
                    s.valid_data()
                        .ok_or_else(|| {
                            anyhow!("Invalid state definition for {s_name} state in service {service_name}")
                        })
                        .and_then(|s| StateWit::try_from_wrapper(s_name, s.to_owned(), types))
                })
                .collect::<Result<_>>()?,
            transforms: TransformsWit {
                steps: self
                    .transforms
                    .into_iter()
                    .map(|step| step.try_into())
                    .collect::<Result<_>>()?,
            },
            post_transforms,
        })
    }
}

impl TryFrom<IoRefWrapper> for IoRefWit {
    type Error = anyhow::Error;
    fn try_from(wrapper: IoRefWrapper) -> Result<Self> {
        let ioref = match wrapper {
            IoRefWrapper::Topic(topic) => Self {
                type_: IoTypeWit::Topic,
                id: topic.id,
                steps: topic
                    .transforms
                    .into_iter()
                    .map(|step| step.try_into())
                    .collect::<Result<_>>()?,
            },
            IoRefWrapper::NoTarget => Self {
                type_: IoTypeWit::NoTarget,
                id: "".into(),
                steps: vec![],
            },
            IoRefWrapper::Schedule(schedule) => Self {
                type_: IoTypeWit::Schedule,
                id: schedule.id,
                steps: schedule
                    .transforms
                    .into_iter()
                    .map(|step| step.try_into())
                    .collect::<Result<_>>()?,
            },
        };
        Ok(ioref)
    }
}

pub(crate) trait IntoTopicBindings {
    fn into_bindings(self, topic_id: &str, defaults: &DefaultConfigs) -> TopicWit;
}

impl IntoTopicBindings for TopicWrapper {
    fn into_bindings(self, topic_id: &str, defaults: &DefaultConfigs) -> TopicWit {
        let with_defaults = self.with_defaults(topic_id, defaults);
        with_defaults.into()
    }
}

impl From<TopicWrapper> for TopicWit {
    fn from(wrapper: TopicWrapper) -> Self {
        Self {
            name: wrapper.name.unwrap_or_default(),
            schema: wrapper.schema.into(),
            consumer: wrapper.consumer.map(|inner| inner.into()),
            producer: wrapper.producer.map(|inner| inner.into()),
            profile: wrapper.remote_cluster_profile,
        }
    }
}

impl From<SchemaWrapper> for TopicSchemaWit {
    fn from(wrapper: SchemaWrapper) -> Self {
        Self {
            key: wrapper.key.map(|inner| inner.into()),
            value: wrapper.value.into(),
        }
    }
}

impl From<SerdeConfig> for SchemaSerDeWit {
    fn from(config: SerdeConfig) -> Self {
        Self {
            type_: TypeRefWit {
                name: config.ty.ty().into(),
            },
            converter: config.converter.map(|inner| inner.into()),
        }
    }
}

impl From<SerdeConverter> for SerdeConverterWit {
    fn from(value: SerdeConverter) -> Self {
        match value {
            SerdeConverter::Json => Self::Json,
            SerdeConverter::Raw => Self::Raw,
        }
    }
}

impl From<ConsumerConfigWrapper> for ConsumerConfigWit {
    fn from(wrapper: ConsumerConfigWrapper) -> Self {
        Self {
            default_starting_offset: wrapper.default_starting_offset.map(|offset| offset.into()),
            max_bytes: wrapper.max_bytes,
            isolation: wrapper.isolation.map(|inner| inner.into()),
        }
    }
}

impl From<ProducerConfigWrapper> for ProducerConfigWit {
    fn from(wrapper: ProducerConfigWrapper) -> Self {
        Self {
            linger_ms: wrapper.linger_ms,
            batch_size_bytes: wrapper.batch_size,
            timeout_ms: wrapper.timeout_ms,
            compression: wrapper.compression.map(|inner| inner.into()),
            isolation: wrapper.isolation.map(|inner| inner.into()),
        }
    }
}

impl From<OffsetWrapper> for OffsetWit {
    fn from(wrapper: OffsetWrapper) -> Self {
        match wrapper {
            OffsetWrapper::Offset(offset) => Self::Offset(offset),
            OffsetWrapper::Beginning(offset) => Self::Beginning(offset),
            OffsetWrapper::End(offset) => Self::End(offset),
        }
    }
}

impl From<Isolation> for IsolationWit {
    fn from(value: Isolation) -> Self {
        match value {
            Isolation::ReadUncommitted => Self::ReadUncommitted,
            Isolation::ReadCommitted => Self::ReadCommitted,
        }
    }
}

impl From<Compression> for CompressionWit {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Gzip => Self::Gzip,
            Compression::Snappy => Self::Snappy,
            Compression::Lz4 => Self::Lz4,
            Compression::Zstd => Self::Zstd,
        }
    }
}

#[cfg(test)]
mod test {

    use super::DataflowDefinitionWit;
    #[test]
    fn test_df_duplicated_types() {
        let config = "
        apiVersion: 0.5.0
        meta:
          name: pkg-types
          version: 0.1.0
          namespace: example
        types:
          my-event:
            type: object
            properties:
              name:
                type: string
              value:
                type: object
                type-name: my-obj
                properties:
                  a:
                    type: string
                  c:
                    type: string
              other-value:
                type: object
                type-name: my-obj
                properties:
                  a:
                    type: string
                  b:
                    type: string
        services:
          my-service:
            sources: []
            sinks: []
            transforms: []
        ";

        let pkg = sdf_parser_df::parse(config).unwrap();
        let wit_pkg = DataflowDefinitionWit::try_from(pkg).expect_err("Expected error");

        assert_eq!(
            wit_pkg.to_string(),
            "Type my-obj is defined multiple times with different definitions"
        );
    }
}
