use serde::{Serialize, Deserialize};

/// Describes where an operator is in a dataflow for add, edit, remove
/// transforms index must be provided if window and partition are false
///
/// if window or partition is true, and transforms index is None,
/// then the OperatorPlacement is referring to the assign_timestamp or assign_key operator
/// respectively
///
/// if window and partition are true, the OperatorPlacement is referring to the
/// partition inside the window
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OperatorPlacement {
    pub service_id: String,
    pub window: bool,
    pub partition: bool,
    pub transforms_index: Option<usize>,
}
