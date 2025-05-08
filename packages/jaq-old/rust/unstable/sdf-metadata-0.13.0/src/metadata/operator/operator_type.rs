use std::fmt::{self, Display, Formatter};

use sdf_common::constants::{
    ASSIGN_KEY_OPERATOR_ID, ASSIGN_TIMESTAMP_OPERATOR_ID, FILTER_MAP_OPERATOR_ID,
    FILTER_OPERATOR_ID, FLAT_MAP_OPERATOR_ID, MAP_OPERATOR_ID, UPDATE_STATE_OPERATOR_ID,
    WINDOW_AGGREGATE_OPERATOR_ID,
};

use crate::wit::operator::TransformOperator;

use crate::wit::operator::OperatorType;

impl From<TransformOperator> for OperatorType {
    fn from(operator: TransformOperator) -> Self {
        match operator {
            TransformOperator::Map(_) => OperatorType::Map,
            TransformOperator::Filter(_) => OperatorType::Filter,
            TransformOperator::FilterMap(_) => OperatorType::FilterMap,
            TransformOperator::FlatMap(_) => OperatorType::FlatMap,
        }
    }
}

impl Display for OperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            OperatorType::Map => write!(f, "{}", MAP_OPERATOR_ID),
            OperatorType::Filter => write!(f, "{}", FILTER_OPERATOR_ID),
            OperatorType::FilterMap => write!(f, "{}", FILTER_MAP_OPERATOR_ID),
            OperatorType::FlatMap => write!(f, "{}", FLAT_MAP_OPERATOR_ID),
            OperatorType::UpdateState => write!(f, "{}", UPDATE_STATE_OPERATOR_ID),
            OperatorType::WindowAggregate => write!(f, "{}", WINDOW_AGGREGATE_OPERATOR_ID),
            OperatorType::AssignTimestamp => write!(f, "{}", ASSIGN_TIMESTAMP_OPERATOR_ID),
            OperatorType::AssignKey => write!(f, "{}", ASSIGN_KEY_OPERATOR_ID),
        }
    }
}
