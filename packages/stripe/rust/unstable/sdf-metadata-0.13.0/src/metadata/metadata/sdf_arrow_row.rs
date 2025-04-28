use std::collections::HashSet;

use anyhow::Result;
use wit_encoder::{Field, Record};

use sdf_common::render::wit_name_case;

use crate::{
    util::config_error::{ConfigError, INDENT},
    wit::metadata::SdfArrowRow,
};

use super::sdf_type::SdfTypeValidationError;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SdfArrowRowValidationError {
    EmptyColumnName,
    DuplicateColumnName(String),
}

impl From<Vec<SdfArrowRowValidationError>> for SdfTypeValidationError {
    fn from(errs: Vec<SdfArrowRowValidationError>) -> Self {
        Self::SdfArrowRow(errs)
    }
}

impl ConfigError for SdfArrowRowValidationError {
    fn readable(&self, indents: usize) -> String {
        let indent = INDENT.repeat(indents);

        match self {
            Self::EmptyColumnName => {
                format!("{}Column name cannot be empty\n", indent)
            }
            Self::DuplicateColumnName(name) => {
                format!(
                    "{}Column name `{}` is duplicated. Column names must be unique\n",
                    indent, name
                )
            }
        }
    }
}

impl SdfArrowRow {
    pub fn validate(&self) -> Result<(), Vec<SdfArrowRowValidationError>> {
        let mut errors = vec![];
        let mut column_names = HashSet::new();

        for column in &self.columns {
            if column.name.is_empty() {
                errors.push(SdfArrowRowValidationError::EmptyColumnName);
            }

            if column_names.contains(&column.name) {
                errors.push(SdfArrowRowValidationError::DuplicateColumnName(
                    column.name.clone(),
                ));
            } else {
                column_names.insert(&column.name);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn wit_record(&self) -> Record {
        let fields = self.columns.iter().map(|field| {
            let name = wit_name_case(&field.name);

            let ty = field.type_.wit_type();

            Field::new(name, ty)
        });

        Record::new(fields)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        metadata::metadata::sdf_arrow_row::SdfArrowRowValidationError,
        util::config_error::ConfigError,
        wit::metadata::{ArrowColumnKind, SdfArrowColumn, SdfArrowRow},
    };

    #[test]
    fn test_validate_accepts_valid_arrow_row() {
        let row = SdfArrowRow {
            columns: vec![SdfArrowColumn {
                name: "number".to_string(),
                type_: ArrowColumnKind::S32,
            }],
        };

        row.validate().expect("should validate");
    }

    #[test]
    fn test_validate_rejects_invalid_arrow_row_column_name() {
        let row = SdfArrowRow {
            columns: vec![SdfArrowColumn {
                name: "".to_string(),
                type_: ArrowColumnKind::S32,
            }],
        };

        let res = row
            .validate()
            .expect_err("should error empty arrow-row column");

        assert!(res.contains(&SdfArrowRowValidationError::EmptyColumnName));
        assert_eq!(res[0].readable(0), "Column name cannot be empty\n")
    }

    #[test]
    fn test_validate_rejects_duplicate_arrow_row_column_names() {
        let row = SdfArrowRow {
            columns: vec![
                SdfArrowColumn {
                    name: "my-column".to_string(),
                    type_: ArrowColumnKind::S32,
                },
                SdfArrowColumn {
                    name: "my-column".to_string(),
                    type_: ArrowColumnKind::S32,
                },
            ],
        };

        let res = row
            .validate()
            .expect_err("should error duplicate column names");

        assert!(
            res.contains(&SdfArrowRowValidationError::DuplicateColumnName(
                "my-column".to_string()
            ))
        );
        assert_eq!(
            res[0].readable(0),
            "Column name `my-column` is duplicated. Column names must be unique\n"
        )
    }
}
