use std::collections::BTreeMap;

use anyhow::{Result, anyhow};

use sdf_common::constants::DATAFLOW_STABLE_VERSION;
use sdf_common::version::ApiVersion;
use sdf_parser_core::config::transform::{TransformOperator, TypedState};
use sdf_parser_core::config::types::{MetadataTypeInner, MetadataTypeTagged, MetadataTypesMap};
use sdf_parser_core::config::DefaultConfigs;
use sdf_parser_package::pkg::{Function, PackageConfig, PackageWrapperV0_5_0};

use crate::wit::metadata::MetadataType;
use crate::wit::{
    package_interface::{PackageDefinition as PackageWit, PackageImport as PackageImportWit},
    dataflow::DevConfig as DevConfigWit,
    states::StateTyped as StateTypedWit,
    metadata::SdfKeyedState as SdfKeyedStateWit,
    operator::OperatorType as OperatorTypeWit,
};
use super::types::IntoBinding;
use super::{IntoTopicBindings, TryIntoBinding};

impl TryFrom<PackageConfig> for PackageWit {
    type Error = anyhow::Error;
    fn try_from(config: PackageConfig) -> Result<Self> {
        match config {
            PackageConfig::V0_4_0(wrapper) => wrapper.try_into_binding(&ApiVersion::V4),
            PackageConfig::V0_5_0(wrapper) => {
                let mut wit_config: PackageWit = wrapper.try_into_binding(&ApiVersion::V5)?;
                wit_config.api_version = DATAFLOW_STABLE_VERSION.to_string();
                Ok(wit_config)
            }
            PackageConfig::V0_6_0(wrapper) => {
                let mut wit_config: PackageWit = wrapper.try_into_binding(&ApiVersion::V6)?;
                wit_config.api_version = DATAFLOW_STABLE_VERSION.to_string();
                Ok(wit_config)
            }
        }
    }
}

impl TryIntoBinding for PackageWrapperV0_5_0 {
    type Target = PackageWit;

    fn try_into_binding(self, api_version: &ApiVersion) -> Result<Self::Target> {
        let types: MetadataTypesMap = self.types.try_into()?;
        let pkg_types = types
            .iter()
            .map(|(name, ty)| ty.to_owned().into_bindings(name, &types, api_version))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<MetadataType>>();

        let mut pkg_types_dedup = BTreeMap::new();
        for ty in pkg_types.iter() {
            if let Some(prev_ty) = pkg_types_dedup.insert(&ty.name, ty.clone()) {
                if prev_ty != *ty {
                    return Err(anyhow!(
                        "Type {} is defined multiple times with different definitions",
                        ty.name
                    ));
                }
            }
        }
        let pkg_types = pkg_types_dedup.into_values().collect::<Vec<_>>();

        Ok(PackageWit {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: self.meta.into(),
            imports: self
                .imports
                .into_iter()
                .map(|import| import.into())
                .collect::<Vec<PackageImportWit>>(),
            functions: self
                .functions
                .iter()
                .map(|operator| {
                    operator
                        .inner()
                        .to_owned()
                        .try_into()
                        .map(|op| (op, operator.to_owned().into()))
                })
                .collect::<Result<_>>()?,
            types: pkg_types,
            states: self
                .states
                .into_iter()
                .map(|(name, s)| StateTypedWit::try_from_wrapper(name, s, &types))
                .collect::<Result<_>>()?,
            dev: self.dev.map(|d| DevConfigWit {
                converter: d.converter.map(|c| c.into()),
                imports: d.imports.into_iter().map(|i| i.into()).collect(),
                topics: d
                    .topics
                    .into_iter()
                    .map(|(key, v)| {
                        (
                            key.clone(),
                            v.into_bindings(&key, &DefaultConfigs::default()),
                        )
                    })
                    .collect(),
            }),
        })
    }
}

impl From<TransformOperator> for OperatorTypeWit {
    fn from(operator: TransformOperator) -> Self {
        match operator {
            TransformOperator::Map(_) => OperatorTypeWit::Map,
            TransformOperator::Filter(_) => OperatorTypeWit::Filter,
            TransformOperator::FilterMap(_) => OperatorTypeWit::FilterMap,
            TransformOperator::FlatMap(_) => OperatorTypeWit::FlatMap,
        }
    }
}

impl From<Function> for OperatorTypeWit {
    fn from(operator: Function) -> Self {
        match operator {
            Function::Map(_) => OperatorTypeWit::Map,
            Function::Filter(_) => OperatorTypeWit::Filter,
            Function::FilterMap(_) => OperatorTypeWit::FilterMap,
            Function::FlatMap(_) => OperatorTypeWit::FlatMap,
            Function::AssignKey(_) => OperatorTypeWit::AssignKey,
            Function::AssignTimestamp(_) => OperatorTypeWit::AssignTimestamp,
            Function::UpdateState(_) => OperatorTypeWit::UpdateState,
            Function::WindowAggregate(_) => OperatorTypeWit::WindowAggregate,
        }
    }
}

impl StateTypedWit {
    fn try_from_wrapper(
        name: String,
        wrapper: TypedState,
        types: &MetadataTypesMap,
    ) -> Result<StateTypedWit> {
        if let MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(key_value)) =
            wrapper.inner_type.ty
        {
            Ok(StateTypedWit {
                name,
                type_: SdfKeyedStateWit::try_from_wrapper(key_value, types)?,
            })
        } else {
            Err(anyhow!(
                "State {} must be a key value type. Found {}",
                name,
                wrapper.inner_type.ty()
            ))
        }
    }
}
#[cfg(test)]
mod test {
    use sdf_parser_package::parse_package;
    use crate::into_wit::pkg_into_wit::PackageWit;

    #[test]
    fn test_pkg_duplicated_types() {
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
  my-obj:
    type: object
    properties:
      a:
        type: string
      b:
        type: string
";

        let pkg = parse_package(config).unwrap();
        let wit_pkg = PackageWit::try_from(pkg).expect_err("Expected error");

        assert_eq!(
            wit_pkg.to_string(),
            "Type my-obj is defined multiple times with different definitions"
        );
    }
}
