use anyhow::Result;

use sdf_common::constants::{
    FILTER_MAP_OPERATOR_ID, FILTER_OPERATOR_ID, FLAT_MAP_OPERATOR_ID, MAP_OPERATOR_ID,
};

use crate::{
    metadata::io::topic::KVSchemaType,
    util::{sdf_types_map::SdfTypesMap, validation_failure::ValidationFailure},
    wit::{
        dataflow::PackageImport,
        operator::{OperatorType, StepInvocation, TransformOperator},
    },
};

impl TransformOperator {
    pub(crate) fn new(
        operator_type: OperatorType,
        step_invocation: StepInvocation,
    ) -> Option<TransformOperator> {
        match operator_type {
            OperatorType::Map => Some(TransformOperator::Map(step_invocation)),
            OperatorType::FilterMap => Some(TransformOperator::FilterMap(step_invocation)),
            OperatorType::Filter => Some(TransformOperator::Filter(step_invocation)),
            OperatorType::FlatMap => Some(TransformOperator::FlatMap(step_invocation)),
            _ => None,
        }
    }

    pub fn is_imported(&self, imports: &[PackageImport]) -> bool {
        self.inner().is_imported(imports)
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Map(step_inv)
            | Self::FilterMap(step_inv)
            | Self::Filter(step_inv)
            | Self::FlatMap(step_inv) => &step_inv.uses,
        }
    }

    pub(crate) fn operator_str(&self) -> &str {
        match self {
            Self::Map(_) => MAP_OPERATOR_ID,
            Self::Filter(_) => FILTER_OPERATOR_ID,
            Self::FilterMap(_) => FILTER_MAP_OPERATOR_ID,
            Self::FlatMap(_) => FLAT_MAP_OPERATOR_ID,
        }
    }

    pub fn inner(&self) -> &StepInvocation {
        match self {
            Self::Map(step_inv)
            | Self::FilterMap(step_inv)
            | Self::Filter(step_inv)
            | Self::FlatMap(step_inv) => step_inv,
        }
    }

    pub fn output_type(&self) -> Option<KVSchemaType> {
        match self {
            Self::Map(step_inv)
            | Self::FilterMap(step_inv)
            | Self::Filter(step_inv)
            | Self::FlatMap(step_inv) => {
                let parameter = step_inv.output.clone();

                parameter.map(|p| p.type_.into())
            }
        }
    }

    pub fn input_type(&self) -> Option<KVSchemaType> {
        match self {
            Self::Map(step_inv)
            | Self::FilterMap(step_inv)
            | Self::Filter(step_inv)
            | Self::FlatMap(step_inv) => {
                if step_inv.requires_key_param() {
                    let key = step_inv.inputs.first();
                    let value = step_inv.inputs.get(1);

                    key.map(|k| {
                        (
                            Some(k.type_.clone()),
                            value.map(|v| v.type_.clone()).unwrap_or_else(|| {
                                panic!("Missing value parameter for operator: {}", self.name())
                            }),
                        )
                            .into()
                    })
                } else {
                    let parameter = step_inv.inputs.first();
                    parameter.map(|p| (None, p.type_.clone()).into())
                }
            }
        }
    }

    pub fn validate(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        match self {
            Self::Map(function) => function.validate_map(types),
            Self::FilterMap(function) => function.validate_filter_map(types),
            Self::Filter(function) => function.validate_filter(types),
            Self::FlatMap(function) => function.validate_flat_map(types),
        }
    }

    #[cfg(feature = "parser")]
    pub fn update_signature_from_code(&mut self) -> Result<()> {
        match self {
            Self::Map(function) => function.update_signature_from_code(),
            Self::FilterMap(function) => function.update_signature_from_code(),
            Self::Filter(function) => function.update_signature_from_code(),
            Self::FlatMap(function) => function.update_signature_from_code(),
        }
    }
}
