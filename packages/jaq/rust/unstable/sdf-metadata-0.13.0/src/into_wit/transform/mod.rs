use anyhow::{anyhow, Context, Result};

use sdf_parser_core::config::transform::code::{Dependency, DependencyVersion};
use sdf_parser_core::config::types::{
    KeyValueType, MetadataType, MetadataTypeInner, MetadataTypeTagged, MetadataTypesMap,
};
use sdf_parser_core::config::utils::parse_to_millis;
use sdf_parser_core::config::transform::{
    Lang, NamedParameterWrapper, ParameterKindWrapper, ParameterWrapper, PartitionOperatorWrapper,
    RefState, StateWrapper, StepInvocationDefinition, StepInvocationWrapperV0_5_0, SystemState,
    TransformOperator, TransformsWrapperV0_5_0, TypedState, WatermarkConfig, WindowKind,
    WindowOperatorWrapper, WindowProperties,
};
use sdf_parser_df::config::PostTransformsInner;

use crate::wit::metadata::{
    OutputType as OutputTypeWit, ParameterKind as ParameterKindWit, SdfKeyValue as SdfKeyValueWit,
};
use crate::into_wit::config::states::{
    StateTyped as StateTypedWit, StateRef as StateRefWit, SystemState as SystemStateWit,
};
use crate::into_wit::config::dataflow::{
    PostTransforms as PostTransformsWit, State as StateWit, Transforms as TransformsWit,
};
use crate::into_wit::config::operator::{
    CodeDep as CodeDepWit, CodeDepVersion as CodeDepVersionWit, CodeInfo as CodeInfoWit,
    CodeLang as CodeLangWit, GitVersion as GitVersionWit, NamedParameter as NamedParameterWit,
    Parameter as ParameterWit, PartitionOperator as PartitionOperatorWit,
    StepInvocation as StepInvocationWit, TransformOperator as TransformOperatorWit,
    TumblingWindow as TumblingWindowWit, Window as WindowWit, SlidingWindow as SlidingWindowWit,
    WindowProperties as WindowPropertiesWit, WindowKind as WindowKindWit,
    WatermarkConfig as WatermarkConfigWit,
};
use crate::into_wit::config::metadata::SdfKeyedState;
use crate::into_wit::config::io::TypeRef as TypeRefWit;

mod code;

impl TryFrom<TransformsWrapperV0_5_0> for TransformsWit {
    type Error = anyhow::Error;
    fn try_from(value: TransformsWrapperV0_5_0) -> Result<Self> {
        Ok(Self {
            steps: value
                .into_iter()
                .map(|step| step.try_into())
                .collect::<Result<_>>()?,
        })
    }
}

impl StateWit {
    pub(crate) fn try_from_wrapper(
        name: String,
        wrapper: StateWrapper,
        types: &MetadataTypesMap,
    ) -> Result<Self> {
        match wrapper {
            StateWrapper::Typed(typed) => StateWit::try_from_typed_wrapper(name, typed, types),
            StateWrapper::Ref(ref_state) => Ok(ref_state.into()),
            StateWrapper::System(system) => Ok((name, system).into()),
        }
    }

    fn try_from_typed_wrapper(
        name: String,
        wrapper: TypedState,
        types: &MetadataTypesMap,
    ) -> Result<StateWit> {
        if let MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(key_value)) =
            wrapper.inner_type.ty
        {
            Ok(StateWit::Typed(StateTypedWit {
                name,
                type_: SdfKeyedState::try_from_wrapper(key_value, types)?,
            }))
        } else {
            Err(anyhow!(
                "State {} must be a key value type. Found {}",
                name,
                wrapper.inner_type.ty()
            ))
        }
    }
}

impl From<RefState> for StateWit {
    fn from(ref_state: RefState) -> StateWit {
        let (ref_service, name) = ref_state.into_pair();
        StateWit::Reference(StateRefWit { ref_service, name })
    }
}

impl From<(String, SystemState)> for StateWit {
    fn from((name, system_state): (String, SystemState)) -> StateWit {
        StateWit::System(SystemStateWit {
            system: system_state.system,
            name,
        })
    }
}

impl TryFrom<StepInvocationWrapperV0_5_0> for StepInvocationWit {
    type Error = anyhow::Error;
    fn try_from(wrapper: StepInvocationWrapperV0_5_0) -> Result<Self, Self::Error> {
        match wrapper.definition {
            StepInvocationDefinition::Code(code) => code.try_into(),
            StepInvocationDefinition::Function(function) => Ok(Self {
                uses: function.uses,
                inputs: function
                    .inputs
                    .into_iter()
                    .map(|input| input.into())
                    .collect(),
                output: function
                    .output
                    .map(|output| output.try_into())
                    .transpose()?,
                states: function
                    .state_imports
                    .into_iter()
                    .map(|s| s.into())
                    .collect(),
                imported_function_metadata: None,
                code_info: CodeInfoWit {
                    lang: CodeLangWit::Rust,
                    code: None,
                    extra_deps: vec![],
                },
                params: Some(function.with.into_iter().collect()),
                ..Default::default()
            }),
        }
    }
}

impl From<Lang> for CodeLangWit {
    fn from(lang: Lang) -> Self {
        match lang {
            Lang::Rust => Self::Rust,
        }
    }
}

impl From<Dependency> for CodeDepWit {
    fn from(dep: Dependency) -> Self {
        Self {
            name: dep.name,
            version: dep.version.into(),
            features: dep.features,
            default_features: dep.default_features,
        }
    }
}

impl From<DependencyVersion> for CodeDepVersionWit {
    fn from(version: DependencyVersion) -> Self {
        match version {
            DependencyVersion::Version { version } => Self {
                version: Some(version),
                path_version: None,
                git_version: None,
            },
            DependencyVersion::Path { path } => Self {
                version: None,
                path_version: Some(path),
                git_version: None,
            },
            DependencyVersion::Git {
                git,
                branch,
                rev,
                tag,
            } => Self {
                version: None,
                path_version: None,
                git_version: Some(GitVersionWit {
                    git,
                    branch,
                    rev,
                    tag,
                }),
            },
        }
    }
}
impl TryFrom<TransformOperator> for TransformOperatorWit {
    type Error = anyhow::Error;
    fn try_from(value: TransformOperator) -> Result<Self> {
        match value {
            TransformOperator::Map(map) => Ok(Self::Map(map.try_into()?)),
            TransformOperator::Filter(filter) => Ok(Self::Filter(filter.try_into()?)),
            TransformOperator::FilterMap(filter_map) => Ok(Self::FilterMap(filter_map.try_into()?)),
            TransformOperator::FlatMap(flat_map) => {
                // make output a list
                let flat_map: StepInvocationWit = flat_map.try_into()?;

                Ok(Self::FlatMap(flat_map))
            }
        }
    }
}

impl From<MetadataType> for TypeRefWit {
    fn from(ty: MetadataType) -> Self {
        let mut name: String = ty.ty().into();

        let list_ty = ty.ty.list_gen_name();

        if name == "list" {
            if let Some(list_ty) = list_ty {
                name = list_ty;
            }
        }
        Self { name }
    }
}

impl From<NamedParameterWrapper> for NamedParameterWit {
    fn from(wrapper: NamedParameterWrapper) -> Self {
        Self {
            name: wrapper.name,
            type_: wrapper.ty.into(),
            optional: wrapper.optional,
            kind: wrapper.kind.into(),
        }
    }
}

impl From<ParameterKindWrapper> for ParameterKindWit {
    fn from(kind: ParameterKindWrapper) -> Self {
        match kind {
            ParameterKindWrapper::Value => Self::Value,
            ParameterKindWrapper::Key => Self::Key,
        }
    }
}

impl TryFrom<ParameterWrapper> for ParameterWit {
    type Error = anyhow::Error;

    fn try_from(wrapper: ParameterWrapper) -> Result<Self> {
        Ok(Self {
            type_: wrapper.ty.try_into()?,
            optional: wrapper.optional,
        })
    }
}

impl TryFrom<MetadataType> for OutputTypeWit {
    type Error = anyhow::Error;
    fn try_from(value: MetadataType) -> Result<Self> {
        match value.ty {
            MetadataTypeInner::NamedType(ty) => Ok(TypeRefWit { name: ty.ty }.into()),
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyValue(key_value)) => {
                Ok(Self::KeyValue(key_value.into()))
            }
            MetadataTypeInner::MetadataTypeTagged(tag) => match tag {
                MetadataTypeTagged::String
                | MetadataTypeTagged::Bool
                | MetadataTypeTagged::Bytes
                | MetadataTypeTagged::Float32
                | MetadataTypeTagged::Float64
                | MetadataTypeTagged::S8
                | MetadataTypeTagged::S16
                | MetadataTypeTagged::S32
                | MetadataTypeTagged::S64
                | MetadataTypeTagged::U8
                | MetadataTypeTagged::U16
                | MetadataTypeTagged::U32
                | MetadataTypeTagged::U64 => Ok(TypeRefWit {
                    name: tag.ty().to_owned(),
                }
                .into()),
                _ => Err(anyhow!("Output type not supported")),
            },
            _ => Err(anyhow!("Output type not supported")),
        }
    }
}

impl From<KeyValueType> for SdfKeyValueWit {
    fn from(kv: KeyValueType) -> Self {
        Self {
            key: TypeRefWit::from(*kv.properties.key),
            value: TypeRefWit::from(*kv.properties.value),
        }
    }
}

impl TryFrom<PartitionOperatorWrapper> for PartitionOperatorWit {
    type Error = anyhow::Error;
    fn try_from(value: PartitionOperatorWrapper) -> Result<Self> {
        Ok(Self {
            assign_key: value.assign_key.try_into()?,
            transforms: TransformsWit {
                steps: value
                    .transforms
                    .into_iter()
                    .map(|step| step.try_into())
                    .collect::<Result<_>>()?,
            },
            update_state: value.update_state.map(|s| s.try_into()).transpose()?,
        })
    }
}
impl TryFrom<WindowOperatorWrapper> for WindowWit {
    type Error = anyhow::Error;
    fn try_from(value: WindowOperatorWrapper) -> Result<Self> {
        Ok(Self {
            properties: value.properties.try_into()?,
            assign_timestamp: value.assign_timestamp.try_into()?,
            transforms: value.transforms.transforms.try_into()?,
            partition: value
                .transforms
                .partition
                .as_ref()
                .map(|p| p.to_owned().try_into())
                .transpose()?,
            flush: value.flush.map(|f| f.clone().try_into()).transpose()?,
        })
    }
}

impl TryFrom<WindowProperties> for WindowPropertiesWit {
    type Error = anyhow::Error;
    fn try_from(value: WindowProperties) -> Result<Self> {
        Ok(Self {
            kind: value.kind.try_into()?,
            watermark_config: value.watermark.try_into()?,
        })
    }
}

impl TryFrom<WatermarkConfig> for WatermarkConfigWit {
    type Error = anyhow::Error;
    fn try_from(value: WatermarkConfig) -> Result<Self> {
        let idleness = value
            .idleness
            .map(|idle| sdf_parser_core::config::utils::parse_to_millis(&idle))
            .transpose()
            .context("Failed to parse idleness config")?;
        let grace_period = value
            .grace_period
            .map(|grace| sdf_parser_core::config::utils::parse_to_millis(&grace))
            .transpose()
            .context("Failed to parse grace_period config")?;
        Ok(Self {
            idleness,
            grace_period,
        })
    }
}

impl TryFrom<WindowKind> for WindowKindWit {
    type Error = anyhow::Error;
    fn try_from(value: WindowKind) -> Result<Self> {
        match value {
            WindowKind::Tumbling(tumbling) => {
                let duration = sdf_parser_core::config::utils::parse_to_millis(&tumbling.duration)
                    .context("failed to parse tumbling duration")?;
                let offset = match tumbling.offset {
                    Some(offset) => parse_to_millis(&offset)?,
                    None => 0u64,
                };

                Ok(Self::Tumbling(TumblingWindowWit { duration, offset }))
            }
            WindowKind::Sliding(sliding) => {
                let duration = sdf_parser_core::config::utils::parse_to_millis(&sliding.duration)
                    .context("failed to parse sliding duration")?;
                let slide = sdf_parser_core::config::utils::parse_to_millis(&sliding.slide)
                    .context("failed to parse slide config")?;
                let offset = match sliding.offset {
                    Some(offset) => parse_to_millis(&offset)?,
                    None => 0u64,
                };

                Ok(Self::Sliding(SlidingWindowWit {
                    duration,
                    offset,
                    slide,
                }))
            }
        }
    }
}

impl TryFrom<PostTransformsInner> for PostTransformsWit {
    type Error = anyhow::Error;
    fn try_from(value: PostTransformsInner) -> Result<Self> {
        match value {
            PostTransformsInner::Partition(partition) => Ok(Self::Partition(partition.try_into()?)),
            PostTransformsInner::Window(window) => Ok(Self::AssignTimestamp(window.try_into()?)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sdf_parser_core::config::transform::TumblingWindow;

    #[test]
    fn test_missing_offset_becomes_zero() {
        let window = WindowProperties {
            kind: WindowKind::Tumbling(TumblingWindow {
                duration: "60seconds".to_string(),
                offset: None,
            }),
            watermark: WatermarkConfig::default(),
        };

        let window_wit: WindowPropertiesWit = window.try_into().expect("convert window");
        match window_wit.kind {
            WindowKindWit::Tumbling(tumbling) => {
                assert_eq!(tumbling.offset, 0u64);
            }
            _ => panic!("Expected tumbling window"),
        }
    }
}
