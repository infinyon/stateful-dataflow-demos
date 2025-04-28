use anyhow::Result;

use crate::{
    metadata::io::topic::KVSchemaType,
    util::{
        sdf_types_map::SdfTypesMap, validation_error::ValidationError,
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::PostTransforms,
        operator::{OperatorType, StepInvocation},
    },
};

impl PostTransforms {
    pub fn operators(&self) -> Vec<(StepInvocation, OperatorType)> {
        match self {
            PostTransforms::AssignTimestamp(window) => window.operators(),
            PostTransforms::Partition(partition) => partition.operators(),
        }
    }
    pub fn output_type(&self, input_type: KVSchemaType) -> Result<KVSchemaType, ValidationError> {
        match self {
            PostTransforms::AssignTimestamp(window) => window.output_type(input_type),
            PostTransforms::Partition(partition) => partition.output_type(input_type),
        }
    }

    pub fn validate(
        &self,
        types: &SdfTypesMap,
        expected_input_type: &KVSchemaType,
    ) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        match self {
            PostTransforms::AssignTimestamp(window) => {
                if let Err(window_error) =
                    window.validate(types, expected_input_type, "transforms block")
                {
                    errors.concat_with_context("Window", &window_error)
                }
            }
            PostTransforms::Partition(partition) => {
                if let Err(partition_error) =
                    partition.validate(types, expected_input_type, "transforms block")
                {
                    errors.concat_with_context("Partition", &partition_error)
                }
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "parser")]
    pub fn update_inline_operators(&mut self) -> Result<()> {
        match self {
            PostTransforms::AssignTimestamp(window) => window.update_inline_operators(),
            PostTransforms::Partition(partition) => partition.update_inline_operators(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::wit::operator::{
        PartitionOperator, TransformOperator, PostTransforms, StepInvocation, Transforms, Window,
    };

    #[test]
    fn test_operators() {
        let post_transforms = PostTransforms::AssignTimestamp(Window {
            partition: Some(PartitionOperator {
                assign_key: StepInvocation {
                    uses: "my-assign".into(),
                    ..Default::default()
                },
                transforms: Transforms {
                    steps: vec![TransformOperator::Map(StepInvocation {
                        uses: "my-map".into(),
                        ..Default::default()
                    })],
                },
                update_state: None,
            }),
            flush: Some(StepInvocation {
                uses: "my-flush".into(),
                ..Default::default()
            }),
            assign_timestamp: StepInvocation {
                uses: "my-assign-timestamp".into(),
                ..Default::default()
            },
            ..Default::default()
        });

        let op = post_transforms.operators();
        assert_eq!(op.len(), 4);
        assert_eq!(op[0].0.uses, "my-assign-timestamp");
        assert_eq!(op[1].0.uses, "my-assign");
        assert_eq!(op[2].0.uses, "my-map");
        assert_eq!(op[3].0.uses, "my-flush");
    }
}
