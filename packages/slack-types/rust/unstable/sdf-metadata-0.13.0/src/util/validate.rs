use crate::wit::metadata::MetadataType;

use super::{
    config_error::{ConfigError, INDENT},
    sdf_types_map::SdfTypesMap,
    validation_error::ValidationError,
    validation_failure::ValidationFailure,
};

use crate::metadata::metadata::sdf_type::SdfTypeValidationError;

pub(crate) trait SimpleValidate {
    fn validate(&self) -> Result<(), ValidationError>;
}

pub(crate) fn validate_all<T: SimpleValidate>(items: &[T]) -> Result<(), ValidationFailure> {
    let mut errors = ValidationFailure::new();

    for item in items {
        if let Err(state_error) = item.validate() {
            errors.push(&state_error);
        }
    }

    if errors.any() {
        Err(errors)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct MetadataTypeValidationFailure {
    pub name: String,
    pub errors: Vec<MetadataTypeValidationError>,
}

impl ConfigError for MetadataTypeValidationFailure {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        let mut result = format!("{}Defined type `{}` is invalid:\n", indent, self.name);

        for error in &self.errors {
            result.push_str(&error.readable(indents + 1));
        }

        result
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum MetadataTypeValidationError {
    EmptyName,
    SdfType(SdfTypeValidationError),
}

impl ConfigError for MetadataTypeValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            MetadataTypeValidationError::EmptyName => {
                format!("{}Name cannot be empty\n", indent)
            }
            MetadataTypeValidationError::SdfType(error) => error.readable(indents),
        }
    }
}

impl MetadataType {
    pub fn validate(&self, types_map: &SdfTypesMap) -> Result<(), MetadataTypeValidationFailure> {
        let mut failure = MetadataTypeValidationFailure {
            name: self.name.to_string(),
            errors: vec![],
        };

        if self.name.is_empty() {
            failure.errors.push(MetadataTypeValidationError::EmptyName);
        }

        if let Err(failures) = self.type_.validate(types_map) {
            failure
                .errors
                .push(MetadataTypeValidationError::SdfType(failures));
        }

        if failure.errors.is_empty() {
            Ok(())
        } else {
            Err(failure)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        metadata::metadata::sdf_type::SdfTypeValidationError,
        util::validate::MetadataTypeValidationError,
        wit::{
            io::TypeRef,
            metadata::{MetadataType, SdfType, SdfTypeOrigin},
        },
    };

    #[test]
    fn test_validate() {
        let type_ = MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Named(TypeRef {
                name: "foobar".to_string(),
            }),
            origin: SdfTypeOrigin::Local,
        };

        let res = type_
            .validate(&Default::default())
            .expect_err("failed to validate");

        assert!(res.errors.contains(&MetadataTypeValidationError::SdfType(
            SdfTypeValidationError::InvalidRef("foobar".to_string())
        )));
    }

    #[test]
    fn test_validate_invalid_ref_state() {
        let type_ = MetadataType {
            name: "my-type".to_string(),
            type_: SdfType::Named(TypeRef {
                name: "arrow-row".to_string(),
            }),
            origin: SdfTypeOrigin::Local,
        };

        let res = type_
            .validate(&Default::default())
            .expect_err("failed to validate");

        assert!(res.errors.contains(&MetadataTypeValidationError::SdfType(
            SdfTypeValidationError::InvalidSyntax("arrow-row".to_string())
        )));
    }
}
