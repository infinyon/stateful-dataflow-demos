use std::path::PathBuf;

use anyhow::{anyhow, Result};

use crate::wit::dataflow::PackageDefinition;
use crate::wit::operator::{StepInvocation, OperatorType, ImportedFunctionMetadata};
use crate::wit::{dataflow::PackageImport, operator::TransformOperator};

pub(crate) fn imported_operator_config(
    operator: &TransformOperator,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
) -> Result<TransformOperator> {
    let function_name = operator.name();

    let (imported_function, imp_op_type) =
        find_imported_function(PathBuf::from("."), function_name, imports, packages)?;

    let operator_type: OperatorType = operator.to_owned().into();

    if operator_type != imp_op_type {
        return Err(anyhow!(
            "Imported function {} is expected to be {} type operator but is {}",
            operator.name(),
            operator.operator_str(),
            imp_op_type.to_string()
        ));
    }

    let overwritten_function =
        inject_imported_function_config(operator.inner(), &imported_function)?;

    Ok(match operator {
        TransformOperator::Map(_) => TransformOperator::Map(overwritten_function),
        TransformOperator::Filter(_) => TransformOperator::Filter(overwritten_function),
        TransformOperator::FilterMap(_) => TransformOperator::FilterMap(overwritten_function),
        TransformOperator::FlatMap(_) => TransformOperator::FlatMap(overwritten_function),
    })
}

fn imported_invoke_config(
    function: &StepInvocation,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
    op_type: OperatorType,
) -> Result<StepInvocation> {
    let function_name = &function.uses;

    let (imported_function, fn_op_type) =
        find_imported_function(PathBuf::from("."), function_name, imports, packages)?;

    if op_type != fn_op_type {
        return Err(anyhow!(
            "Imported function {} is expected to be {} but is {}",
            function_name,
            op_type,
            fn_op_type.to_string()
        ));
    }

    inject_imported_function_config(function, &imported_function)
}

pub(crate) fn imported_assign_key_config(
    function: &StepInvocation,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
) -> Result<StepInvocation> {
    imported_invoke_config(function, imports, packages, OperatorType::AssignKey)
}

pub(crate) fn imported_update_state_config(
    function: &StepInvocation,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
) -> Result<StepInvocation> {
    imported_invoke_config(function, imports, packages, OperatorType::UpdateState)
}

pub(crate) fn imported_assign_timestamp_config(
    function: &StepInvocation,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
) -> Result<StepInvocation> {
    imported_invoke_config(function, imports, packages, OperatorType::AssignTimestamp)
}

fn find_imported_function(
    previous_path: PathBuf,
    function_name: &str,
    imports: &[PackageImport],
    packages: &[PackageDefinition],
) -> Result<(StepInvocation, OperatorType)> {
    // find the function's real name and origin package
    let ImportedFunctionMetadata {
        original_name,
        package_path,
        package_metadata,
    } = imported_package_meta_for_function(function_name, imports)?;

    // get the package config
    let package = packages
        .iter()
        .find(|p| package_metadata == p.meta)
        .ok_or_else(|| {
            anyhow!(
                "Package {}/{}:{} not found in packages",
                package_metadata.namespace,
                package_metadata.name,
                package_metadata.version
            )
        })?;

    // if the function is in the defined functions, return it
    for function in package.functions.iter() {
        if function.0.uses == original_name {
            let mut result_function = function.clone();

            result_function.0.imported_function_metadata = Some(ImportedFunctionMetadata {
                original_name: original_name.clone(),
                package_path: previous_path.join(package_path).display().to_string(),
                package_metadata: package_metadata.clone(),
            });

            return Ok(result_function);
        }
    }

    // otherwise, find the function in the package's imports
    find_imported_function(
        previous_path.join(package_path),
        &original_name,
        &package.imports,
        packages,
    )
}

fn imported_package_meta_for_function(
    function_name: &str,
    imports: &[PackageImport],
) -> Result<ImportedFunctionMetadata> {
    for import in imports {
        let import_path = import_path(import)?;

        for function in &import.functions {
            if let Some(alias) = &function.alias {
                if function_name == alias {
                    return Ok(ImportedFunctionMetadata {
                        original_name: function.name.clone(),
                        package_path: import_path.clone(),
                        package_metadata: import.metadata.clone(),
                    });
                } else {
                    // if there is an alias, the original function name is ignored
                    continue;
                }
            } else if function_name == function.name {
                return Ok(ImportedFunctionMetadata {
                    original_name: function.name.clone(),
                    package_path: import_path.clone(),
                    package_metadata: import.metadata.clone(),
                });
            }
        }
    }

    Err(anyhow!("Function {} not found in imports", function_name))
}

fn inject_imported_function_config(
    dataflow_defined_function: &StepInvocation,
    imported_function: &StepInvocation,
) -> Result<StepInvocation> {
    let mut function = dataflow_defined_function.clone();

    function.set_inputs(imported_function.inputs.clone());
    function.set_output(imported_function.output.clone());
    function.set_states(imported_function.states.clone());
    function.set_imported_function_metadata(imported_function.imported_function_metadata.clone());

    Ok(function)
}

fn import_path(import: &PackageImport) -> Result<&String> {
    import
        .path
        .as_ref()
        .ok_or(anyhow!("Import must have path when resolving functions"))
}

#[cfg(test)]
mod test {

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use super::{
        imported_operator_config, imported_assign_key_config, imported_assign_timestamp_config,
    };

    use crate::wit::dataflow::PackageDefinition;
    use crate::wit::operator::{StepInvocation, OperatorType, ImportedFunctionMetadata};
    use crate::wit::package_interface::{Header, FunctionImport, PackageImport};
    use crate::wit::operator::TransformOperator;
    use crate::wit::metadata::{NamedParameter, Parameter, ParameterKind, TypeRef};

    fn packages() -> Vec<PackageDefinition> {
        vec![
            PackageDefinition {
                api_version: DATAFLOW_STABLE_VERSION.to_string(),
                meta: Header {
                    name: "first-pkg".to_string(),
                    namespace: "first-ns".to_string(),
                    version: "0.1.0".to_string(),
                },
                functions: vec![(
                    StepInvocation {
                        uses: "first-fn".to_string(),
                        inputs: vec![NamedParameter {
                            name: "first-input".to_string(),
                            type_: TypeRef {
                                name: "u16".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "u16".to_string(),
                            }
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    OperatorType::Map,
                )],
                imports: vec![PackageImport {
                    metadata: Header {
                        name: "second-pkg".to_string(),
                        namespace: "second-ns".to_string(),
                        version: "0.1.0".to_string(),
                    },
                    functions: vec![FunctionImport {
                        name: "second-fn".to_string(),
                        alias: Some("foobar-fn".to_string()),
                    }],
                    path: Some("../second_pkg".to_string()),
                    types: vec![],
                    states: vec![],
                }],
                types: vec![],
                states: vec![],
                dev: None,
            },
            PackageDefinition {
                api_version: DATAFLOW_STABLE_VERSION.to_string(),
                meta: Header {
                    name: "second-pkg".to_string(),
                    namespace: "second-ns".to_string(),
                    version: "0.1.0".to_string(),
                },
                functions: vec![(
                    StepInvocation {
                        uses: "second-fn".to_string(),
                        inputs: vec![NamedParameter {
                            name: "second-input".to_string(),
                            type_: TypeRef {
                                name: "u8".to_string(),
                            },
                            optional: false,
                            kind: ParameterKind::Value,
                        }],
                        output: Some(Parameter {
                            type_: TypeRef {
                                name: "u8".to_string(),
                            }
                            .into(),
                            optional: true,
                        }),
                        ..Default::default()
                    },
                    OperatorType::Filter,
                )],
                imports: vec![],
                types: vec![],
                states: vec![],
                dev: None,
            },
        ]
    }

    fn imports() -> Vec<PackageImport> {
        vec![PackageImport {
            metadata: Header {
                name: "first-pkg".to_string(),
                namespace: "first-ns".to_string(),
                version: "0.1.0".to_string(),
            },
            functions: vec![
                FunctionImport {
                    name: "first-fn".to_string(),
                    alias: Some("alias-first-fn".to_string()),
                },
                FunctionImport {
                    name: "foobar-fn".to_string(),
                    alias: None,
                },
            ],
            path: Some("../packages/first_pkg".to_string()),
            types: vec![],
            states: vec![],
        }]
    }

    #[test]
    fn test_imported_operator_finds_imported_fn() {
        let map_operator = TransformOperator::Map(StepInvocation {
            uses: "alias-first-fn".to_string(),
            ..Default::default()
        });

        let overwritten_operator = imported_operator_config(&map_operator, &imports(), &packages())
            .expect("should import operator");

        assert_eq!(
            overwritten_operator,
            TransformOperator::Map(StepInvocation {
                uses: "alias-first-fn".to_string(),
                inputs: vec![NamedParameter {
                    name: "first-input".to_string(),
                    type_: TypeRef {
                        name: "u16".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "u16".to_string(),
                    }
                    .into(),
                    ..Default::default()
                }),
                imported_function_metadata: Some(ImportedFunctionMetadata {
                    original_name: "first-fn".to_string(),
                    package_path: "./../packages/first_pkg".to_string(),
                    package_metadata: Header {
                        name: "first-pkg".to_string(),
                        namespace: "first-ns".to_string(),
                        version: "0.1.0".to_string(),
                    },
                }),
                ..Default::default()
            },)
        );
    }

    #[test]
    fn test_imported_operator_finds_repeatedly_imported_fn() {
        let filter_operator = TransformOperator::Filter(StepInvocation {
            uses: "foobar-fn".to_string(),
            ..Default::default()
        });

        let overwritten_operator =
            imported_operator_config(&filter_operator, &imports(), &packages())
                .expect("should import operator");

        assert_eq!(
            overwritten_operator,
            TransformOperator::Filter(StepInvocation {
                uses: "foobar-fn".to_string(),
                inputs: vec![NamedParameter {
                    name: "second-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                }],
                output: Some(Parameter {
                    type_: TypeRef {
                        name: "u8".to_string(),
                    }
                    .into(),
                    optional: true,
                }),
                imported_function_metadata: Some(ImportedFunctionMetadata {
                    original_name: "second-fn".to_string(),
                    package_path: "./../packages/first_pkg/../second_pkg".to_string(),
                    package_metadata: Header {
                        name: "second-pkg".to_string(),
                        namespace: "second-ns".to_string(),
                        version: "0.1.0".to_string(),
                    }
                }),
                ..Default::default()
            },)
        );
    }

    #[test]
    fn test_imported_operator_errors_on_op_type_mismatch() {
        let filter_operator = TransformOperator::Map(StepInvocation {
            uses: "foobar-fn".to_string(),
            ..Default::default()
        });

        let error = imported_operator_config(&filter_operator, &imports(), &packages())
            .expect_err("Should fail on op type mismatch");

        assert_eq!(
            format!("{}", error.root_cause()),
            "Imported function foobar-fn is expected to be map type operator but is filter"
        );
    }

    #[test]
    fn test_imported_operator_errors_on_fn_not_imported() {
        let operator = TransformOperator::Map(StepInvocation {
            uses: "non-existent".to_string(),
            ..Default::default()
        });

        let error = imported_operator_config(&operator, &imports(), &packages())
            .expect_err("Should fail on import not found");

        assert_eq!(
            format!("{}", error.root_cause()),
            "Function non-existent not found in imports"
        );
    }

    #[test]
    fn test_imported_operator_errors_on_package_missing() {
        let filter_operator = TransformOperator::Map(StepInvocation {
            uses: "alias-first-fn".to_string(),
            ..Default::default()
        });

        let error = imported_operator_config(&filter_operator, &imports(), &[])
            .expect_err("Should fail on package not found");

        assert_eq!(
            format!("{}", error.root_cause()),
            "Package first-ns/first-pkg:0.1.0 not found in packages"
        );
    }

    #[test]
    fn test_imported_assign_timestamp_config_errors_on_wrong_op_type() {
        let assign_timestamp_function = StepInvocation {
            uses: "alias-first-fn".to_string(),
            ..Default::default()
        };

        let error =
            imported_assign_timestamp_config(&assign_timestamp_function, &imports(), &packages())
                .expect_err("Should fail on wrong op type");

        assert_eq!(
            format!("{}", error.root_cause()),
            "Imported function alias-first-fn is expected to be assign-timestamp but is map"
        );
    }

    #[test]
    fn test_imported_assign_key_conifg_errors_on_wrong_op_type() {
        let assign_key_function = StepInvocation {
            uses: "alias-first-fn".to_string(),
            ..Default::default()
        };

        let error = imported_assign_key_config(&assign_key_function, &imports(), &packages())
            .expect_err("Should fail on wrong op type");

        assert_eq!(
            format!("{}", error.root_cause()),
            "Imported function alias-first-fn is expected to be assign-key but is map"
        );
    }
}
