use serde::{Deserialize, Serialize};

use super::{import::StateImport, types::MetadataType};
use self::code::Dependency;
pub use self::code::{FunctionDefinition, StepInvocationDefinition, Lang};

pub mod code;

pub type TransformsWrapperV0_5_0 = Vec<TransformOperator>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WindowOperatorWrapper {
    #[serde(flatten)]
    pub properties: WindowProperties,
    #[serde(alias = "assign-timestamp")]
    pub assign_timestamp: StepInvocationWrapperV0_5_0,
    pub flush: Option<StepInvocationWrapperV0_5_0>,
    #[serde(flatten)]
    pub transforms: WindowTransforms,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WindowTransforms {
    #[serde(default)]
    pub transforms: TransformsWrapperV0_5_0,
    #[serde(default)]
    pub partition: Option<PartitionOperatorWrapper>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartitionOperatorWrapper {
    #[serde(alias = "assign-key")]
    pub assign_key: StepInvocationWrapperV0_5_0,
    #[serde(default)]
    pub transforms: TransformsWrapperV0_5_0,
    #[serde(alias = "update-state", default)]
    pub update_state: Option<StepInvocationWrapperV0_5_0>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum StateWrapper {
    Ref(RefState),
    System(SystemState),
    Typed(TypedState),
}

impl StateWrapper {
    pub fn inner_type(&self) -> Option<&MetadataType> {
        match self {
            Self::Typed(typed) => Some(&typed.inner_type),
            Self::Ref(_) => None,
            Self::System(_) => None,
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TypedState {
    #[serde(flatten)]
    pub inner_type: MetadataType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RefState {
    pub from: String,
}

impl RefState {
    pub fn into_pair(&self) -> (String, String) {
        let mut iter = self.from.split('.');
        let service = iter
            .next()
            .expect("Shouldn't fail if called after validate")
            .to_string();
        let state = iter
            .next()
            .expect("Shouldn't fail if called after validate")
            .to_string();

        (service, state)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SystemState {
    pub system: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct StepInvocationWrapperV0_5_0 {
    #[serde(flatten)]
    pub definition: StepInvocationDefinition,
}

impl StepInvocationWrapperV0_5_0 {
    pub fn state_imports(&self) -> &Vec<StateImport> {
        match &self.definition {
            StepInvocationDefinition::Code(code) => &code.state_imports,
            StepInvocationDefinition::Function(function) => &function.state_imports,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "operator")]
#[serde(rename_all = "kebab-case")]
pub enum TransformOperator {
    Map(StepInvocationWrapperV0_5_0),
    Filter(StepInvocationWrapperV0_5_0),
    FilterMap(StepInvocationWrapperV0_5_0),
    FlatMap(StepInvocationWrapperV0_5_0),
}

impl TransformOperator {
    pub fn extra_deps(&self) -> Vec<Dependency> {
        self.inner().definition.extra_deps()
    }

    pub fn inner(&self) -> &StepInvocationWrapperV0_5_0 {
        match self {
            Self::Map(map) => map,
            Self::Filter(filter) => filter,
            Self::FilterMap(filter_map) => filter_map,
            Self::FlatMap(flat_map) => flat_map,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NamedParameterWrapper {
    pub name: String,
    #[serde(flatten)]
    pub ty: MetadataType,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub kind: ParameterKindWrapper,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum ParameterKindWrapper {
    Key,
    #[default]
    Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParameterWrapper {
    #[serde(flatten)]
    pub ty: MetadataType,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub list: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WindowProperties {
    #[serde(flatten)]
    pub kind: WindowKind,
    #[serde(default)]
    pub watermark: WatermarkConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum WindowKind {
    Tumbling(TumblingWindow),
    Sliding(SlidingWindow),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TumblingWindow {
    pub duration: String,
    pub offset: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlidingWindow {
    pub duration: String,
    pub offset: Option<String>,
    pub slide: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct WatermarkConfig {
    pub idleness: Option<String>,
    #[serde(alias = "grace_period")]
    pub grace_period: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum OperatorAdaptor {
    Http,
}

#[cfg(test)]
mod yaml_test {
    use std::collections::BTreeMap;

    use super::StateWrapper;

    #[test]
    fn parse_test_system_wrapper() {
        let yaml = r#"
        system: cmd
        name: my-state
        "#;

        let state: StateWrapper = serde_yaml::from_str(yaml).unwrap();

        match state {
            StateWrapper::System(sys_state) => {
                assert_eq!(sys_state.system, "cmd");
            }
            _ => panic!("failed to parse state wrapper"),
        }
    }

    #[test]
    fn parse_ref_state_wrapper() {
        let yaml = r#"
        from: another.state
        "#;

        let state: StateWrapper = serde_yaml::from_str(yaml).unwrap();

        match state {
            StateWrapper::Ref(rf) => {
                assert_eq!(rf.from, "another.state");
            }
            _ => panic!("failed to parse state wrapper"),
        }
    }

    #[test]
    fn parse_type_state_wrapper() {
        let yaml = r#"
        my-state:
          type: keyed-state
          properties:
            key:
              type: string
            value:
              type: string
        "#;

        let state: BTreeMap<String, StateWrapper> = serde_yaml::from_str(yaml).unwrap();

        match state.get("my-state").unwrap() {
            StateWrapper::Typed(_) => {}
            StateWrapper::Ref(_) => panic!("parse as ref state"),
            StateWrapper::System(_) => panic!("parse as system state"),
        }
    }
}
