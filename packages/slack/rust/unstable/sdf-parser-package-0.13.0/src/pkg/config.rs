use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, KeyValueMap};

use sdf_parser_core::config::{
    dev::DevConfig,
    import::{PackageImport, PackageMetadata},
    transform::TypedState,
    types::MetadataTypesMapWrapper,
};

use super::functions::Function;

pub fn parse_package(pkg: &str) -> anyhow::Result<PackageConfig> {
    let yd = serde_yaml::Deserializer::from_str(pkg);
    let config = serde_path_to_error::deserialize(yd)?;

    Ok(config)
}

pub type CurrentPkgConfig = PackageWrapperV0_5_0;
pub type DevPkgConfig = PackageWrapperV0_5_0;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "apiVersion")]
pub enum PackageConfig {
    #[serde(rename = "0.4.0")]
    V0_4_0(CurrentPkgConfig),
    #[serde(rename = "0.5.0")]
    V0_5_0(DevPkgConfig),
    #[serde(rename = "0.6.0")]
    V0_6_0(DevPkgConfig),
}

impl PackageConfig {
    pub fn is_v5(&self) -> bool {
        matches!(self, Self::V0_5_0(_))
    }

    pub fn is_v4(&self) -> bool {
        matches!(self, Self::V0_4_0(_))
    }

    pub fn imports(&self) -> &Vec<PackageImport> {
        match self {
            Self::V0_4_0(wrapper) => &wrapper.imports,
            Self::V0_5_0(wrapper) => &wrapper.imports,
            Self::V0_6_0(wrapper) => &wrapper.imports,
        }
    }

    pub fn types(&self) -> &MetadataTypesMapWrapper {
        match self {
            Self::V0_4_0(wrapper) => &wrapper.types,
            Self::V0_5_0(wrapper) => &wrapper.types,
            Self::V0_6_0(wrapper) => &wrapper.types,
        }
    }

    pub fn states(&self) -> &BTreeMap<String, TypedState> {
        match self {
            Self::V0_4_0(wrapper) => &wrapper.states,
            Self::V0_5_0(wrapper) => &wrapper.states,
            Self::V0_6_0(wrapper) => &wrapper.states,
        }
    }

    pub fn functions(&self) -> &Vec<Function> {
        match self {
            Self::V0_4_0(wrapper) => &wrapper.functions,
            Self::V0_5_0(wrapper) => &wrapper.functions,
            Self::V0_6_0(wrapper) => &wrapper.functions,
        }
    }

    pub fn dev(&self) -> Option<&DevConfig> {
        match self {
            Self::V0_4_0(_) => None,
            Self::V0_5_0(wrapper) => wrapper.dev.as_ref(),
            Self::V0_6_0(wrapper) => wrapper.dev.as_ref(),
        }
    }
}

impl Deref for PackageConfig {
    type Target = CurrentPkgConfig;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::V0_4_0(wrapper) => wrapper,
            Self::V0_5_0(wrapper) => wrapper,
            Self::V0_6_0(wrapper) => wrapper,
        }
    }
}

impl DerefMut for PackageConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::V0_4_0(wrapper) => wrapper,
            Self::V0_5_0(wrapper) => wrapper,
            Self::V0_6_0(wrapper) => wrapper,
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PackageWrapperV0_5_0 {
    pub meta: PackageMetadata,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub imports: Vec<PackageImport>,
    #[serde(default)]
    pub types: MetadataTypesMapWrapper,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub states: BTreeMap<String, TypedState>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    #[serde_as(as = "KeyValueMap<_>")]
    pub functions: Vec<Function>,
    pub dev: Option<DevConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PackageUnsupportedVersion {
    pub meta: PackageMetadata,
}
#[cfg(test)]
mod tests {

    use sdf_parser_core::{
        config::{
            transform::{Lang, StepInvocationDefinition},
            types::{MetadataTypeInner, MetadataTypeTagged, NamedType},
            SerdeConverter,
        },
        MaybeValid,
    };

    use super::*;

    #[test]
    fn test_parse_package() {
        let yaml = "
apiVersion: 0.5.0
meta:
  name: my-package
  version: 0.1.0
  namespace: example

types:
  sentence:
    type: string

states:
  count-per-model:
    type: keyed-state
    properties:
      key:
        type: string
      value:
        type: u32

functions:
  my-hello-fn:
    operator: filter-map
    language: rust
    inputs:
      - name: input
        type: sentence
    output:
      type: string

dev:
  converter: raw    # options: raw, json
"
        .to_string();

        let config = parse_package(&yaml).expect("should validate");

        assert_eq!(config.meta.name, "my-package");
        assert_eq!(config.meta.version, "0.1.0");
        assert_eq!(config.meta.namespace, "example");

        let types = &config.types;
        let sentence_ty = types.map.get("sentence").expect("type to be found");
        assert_eq!(
            sentence_ty,
            &MaybeValid::Valid(
                MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
            )
        );

        let states = &config.states;
        assert_eq!(states.len(), 1);

        config
            .states
            .iter()
            .find(|(state_name, _)| *state_name == "count-per-model")
            .expect("State to have parsed");

        let function = config.functions.first().expect("should have a function");

        match &function.inner().definition {
            StepInvocationDefinition::Function(function) => {
                assert_eq!(Lang::Rust, function.lang);
                assert_eq!(function.uses, "my-hello-fn");
                assert_eq!(function.inputs.len(), 1);
                assert_eq!(function.inputs[0].name, "input");
                assert_eq!(
                    function.inputs[0].ty,
                    MetadataTypeInner::NamedType(NamedType {
                        ty: "sentence".to_string()
                    })
                    .into()
                );
                assert_eq!(
                    function.output.as_ref().unwrap().ty,
                    MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
                );
            }
            _ => panic!("incorrect function type parsed"),
        }

        assert_eq!(
            config.dev.as_ref().unwrap().converter,
            Some(SerdeConverter::Raw)
        );
    }

    #[test]
    fn test_valid_imports_validate() {
        let yaml = "
apiVersion: 0.5.0
meta:
  name: my-package
  version: 0.1.0
  namespace: example

imports:
  - pkg: example/bank-types@0.1.0
    types:
      - name: bank-event
    states:
      - name: account-balance

functions:
  update-bank-account:
    operator: update-state
    language: rust
    states:
      - name: account-balance
    inputs:
      - name: input
        type: string

dev:
  converter: json
  imports:
    - pkg: example/bank-types@0.1.0
      path: ../bank-types
"
        .to_string();

        let config: PackageConfig = serde_yaml::from_str(&yaml).expect("function to parse");

        let import = config.imports.first().expect("Should have an import");

        assert_eq!(import.package.namespace, "example");
        assert_eq!(import.package.name, "bank-types");
        assert_eq!(import.package.version, "0.1.0");
        assert_eq!(import.types[0].name, "bank-event");
        assert_eq!(import.states[0].name, "account-balance");

        let function = config.functions.first().expect("Should have a function");

        match &function.inner().definition {
            StepInvocationDefinition::Function(function) => {
                assert_eq!(Lang::Rust, function.lang);
                assert_eq!(function.uses, "update-bank-account");
                assert_eq!(function.state_imports[0].name, "account-balance");
                assert_eq!(function.inputs[0].name, "input");
                assert_eq!(
                    function.inputs[0].ty,
                    MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
                );
            }
            _ => panic!("incorrect function type parsed"),
        }

        let dev_config = config.dev.as_ref().expect("Should have dev config");

        assert_eq!(dev_config.imports[0].package.namespace, "example");
        assert_eq!(dev_config.imports[0].package.name, "bank-types");
        assert_eq!(dev_config.imports[0].package.version, "0.1.0");
        assert_eq!(
            dev_config.imports[0].path,
            Some(String::from("../bank-types"))
        );
    }

    #[test]
    fn test_import_names_are_validated() {
        let yaml = "
apiVersion: 0.5.0
meta:
  name: my-package
  version: 0.1.0
  namespace: example

imports:
  - pkg: bank-types@0.1.0
    types:
      - name: bank-event
    states:
      - name: account-balance

functions:
  update-bank-account:
    operator: update-state
    language: rust
    states:
      - name: account-balance
    inputs:
      - name: input
        type: string

dev:
  converter: json
  imports:
    - pkg: example/bank-types@0.1.0
      path: ../bank-types
"
        .to_string();

        let error = serde_yaml::from_str::<PackageConfig>(&yaml).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid value: string \"bank-types@0.1.0\", expected a string of the form `<namespace>/<name>@<version>`"
        );
    }

    #[test]
    fn test_api_version() {
        let v5_yaml = "
apiVersion: 0.5.0
meta:
  name: my-package
  version: 0.1.0
  namespace: example
"
        .to_string();

        let config: PackageConfig = serde_yaml::from_str(&v5_yaml).expect("function to parse");
        assert!(config.is_v5());
        assert!(!config.is_v4());
        drop(config);

        let v4_yaml = "
        apiVersion: 0.4.0
        meta:
          name: my-package
          version: 0.1.0
          namespace: example

        "
        .to_string();

        let config: PackageConfig = serde_yaml::from_str(&v4_yaml).expect("function to parse");
        assert!(!config.is_v5());
        assert!(config.is_v4());
    }
}
