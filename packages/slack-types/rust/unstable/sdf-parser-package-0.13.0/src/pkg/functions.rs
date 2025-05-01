use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sdf_parser_core::config::transform::{code::Dependency, StepInvocationWrapperV0_5_0};

/// All functions that can be used in the package
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(tag = "operator")]
#[serde(rename_all = "kebab-case")]
pub enum Function {
    Map(StepInvocationWrapperV0_5_0),
    Filter(StepInvocationWrapperV0_5_0),
    FilterMap(StepInvocationWrapperV0_5_0),
    FlatMap(StepInvocationWrapperV0_5_0),
    AssignKey(StepInvocationWrapperV0_5_0),
    AssignTimestamp(StepInvocationWrapperV0_5_0),
    UpdateState(StepInvocationWrapperV0_5_0),
    #[serde(rename = "aggregate")]
    WindowAggregate(StepInvocationWrapperV0_5_0),
}

impl Function {
    pub fn name(&self) -> Option<&str> {
        self.inner().definition.name()
    }
    pub fn extra_deps(&self) -> Vec<Dependency> {
        self.inner().definition.extra_deps()
    }

    pub fn inner(&self) -> &StepInvocationWrapperV0_5_0 {
        match self {
            Self::Map(map) => map,
            Self::Filter(filter) => filter,
            Self::FilterMap(filter_map) => filter_map,
            Self::FlatMap(flat_map) => flat_map,
            Self::AssignKey(assign_key) => assign_key,
            Self::AssignTimestamp(assign_timestamp) => assign_timestamp,
            Self::UpdateState(update_state) => update_state,
            Self::WindowAggregate(window_aggregate) => window_aggregate,
        }
    }
}
