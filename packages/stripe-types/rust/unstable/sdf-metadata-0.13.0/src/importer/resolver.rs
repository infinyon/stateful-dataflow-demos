use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};

use crate::wit::package_interface::{PackageDefinition, PackageImport};

impl DependencyNode {
    pub fn resolve(
        &mut self,
        packages: &BTreeMap<String, Arc<Mutex<DependencyNode>>>,
    ) -> Result<()> {
        match self.resolution {
            NodeResolution::Resolved => Ok(()),
            NodeResolution::Unresolved => {
                let mut children = vec![];
                for import in self.pkg.imports.iter() {
                    let key = format!(
                        "{}:{}:{}",
                        import.metadata.namespace, import.metadata.name, import.metadata.version
                    );
                    let dep = match packages.get(&key) {
                        Some(dep) => dep,
                        None => {
                            return Err(anyhow!("Could not find package with key: {}", key));
                        }
                    };

                    let Ok(mut dep_lock) = dep.lock() else {
                        return Err(anyhow!("Could not lock package with key: {}", key));
                    };

                    dep_lock.resolve(packages)?;

                    children.push(dep_lock.pkg.clone());
                }

                self.pkg.merge_dependencies(&children)?;

                self.resolution = NodeResolution::Resolved;

                Ok(())
            }
        }
    }
}

pub struct DependencyNode {
    resolution: NodeResolution,
    pkg: PackageDefinition,
}

impl DependencyNode {
    pub fn new(pkg: PackageDefinition) -> Self {
        DependencyNode {
            resolution: NodeResolution::Unresolved,
            pkg,
        }
    }
}

pub enum NodeResolution {
    /// A node is resolved when all of its dependencies have been resolved
    /// and it merges its local types with the imported types
    Resolved,
    Unresolved,
}

/// DependencyResolver is a struct that for a list import declarations and package configs
/// will resolve the dependencies between the packages.
pub struct DependencyResolver {
    imports: Vec<PackageImport>,
    packages: BTreeMap<String, Arc<Mutex<DependencyNode>>>,
}

impl DependencyResolver {
    /// build and resolve the dependency tree for a list of imports
    pub fn build(
        imports: Vec<PackageImport>,
        package_configs: Vec<PackageDefinition>,
        _debug: bool,
    ) -> Result<Self> {
        let mut packages = BTreeMap::new();
        for package in package_configs.iter() {
            packages.insert(
                format!(
                    "{}:{}:{}",
                    package.meta.namespace, package.meta.name, package.meta.version
                ),
                Arc::new(Mutex::new(DependencyNode::new(package.clone()))),
            );
        }

        let mut tree = DependencyResolver { imports, packages };

        tree.resolve()?;
        Ok(tree)
    }

    fn resolve(&mut self) -> Result<()> {
        for import in self.imports.iter() {
            let key = format!(
                "{}:{}:{}",
                import.metadata.namespace, import.metadata.name, import.metadata.version
            );
            let dep = match self.packages.get(&key) {
                Some(dep) => dep,
                None => {
                    return Err(anyhow!("Could not find package with key: {}", key));
                }
            };

            let Ok(mut dep_lock) = dep.lock() else {
                return Err(anyhow!("Could not lock package with key: {}", key));
            };

            dep_lock.resolve(&self.packages)?;
        }

        Ok(())
    }

    /// get the resolved packages
    pub fn packages(&self) -> Result<Vec<PackageDefinition>> {
        let pkgs = self
            .packages
            .values()
            .map(|pkg| {
                let Ok(pkg_lock) = pkg.lock() else {
                    return Err(anyhow!("Could not lock package"));
                };

                Ok(pkg_lock.pkg.clone())
            })
            .collect::<Result<Vec<PackageDefinition>>>()?;
        Ok(pkgs)
    }
}

#[cfg(test)]
mod test {

    use sdf_common::constants::DATAFLOW_STABLE_VERSION;

    use crate::wit::io::SerdeConverter;
    use crate::wit::metadata::{
        ArrowColumnKind, NamedParameter, ObjectField, ParameterKind, SdfArrowColumn, SdfArrowRow,
        SdfKeyedState, SdfKeyedStateValue, SdfObject, SdfTypeOrigin, TypeRef, SerdeConfig,
    };
    use crate::wit::package_interface::{
        DevConfig, PackageDefinition, PackageImport, Header, StateTyped,
    };
    use crate::wit::operator::{OperatorType, StateImport, StepState, StepInvocation};

    use crate::wit::metadata::{MetadataType, SdfType};

    use super::DependencyResolver;

    fn first_package_definition() -> PackageDefinition {
        PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                namespace: "example".to_string(),
                name: "bank-update".to_string(),
                version: "0.1.0".to_string(),
            },
            imports: vec![PackageImport {
                metadata: Header {
                    namespace: "example".to_string(),
                    name: "bank".to_string(),
                    version: "0.1.0".to_string(),
                },
                types: vec!["bank-event".to_string(), "bank-account".to_string()],
                states: vec!["account-balance".to_string()],
                path: Some("../bank-types".to_string()),
                functions: vec![],
            }],
            functions: vec![(
                StepInvocation {
                    uses: "filter-position-events".to_string(),
                    inputs: vec![NamedParameter {
                        name: "event".to_string(),
                        type_: TypeRef {
                            name: "bank-event".to_string(),
                        },
                        optional: false,
                        kind: ParameterKind::Value,
                    }],
                    states: vec![StepState::Unresolved(StateImport {
                        name: "account-balance".to_string(),
                    })],
                    ..Default::default()
                },
                OperatorType::Filter,
            )],
            dev: Some(DevConfig {
                converter: Some(SerdeConverter::Json),
                imports: vec![PackageImport {
                    metadata: Header {
                        namespace: "example".to_string(),
                        name: "bank".to_string(),
                        version: "0.1.0".to_string(),
                    },
                    path: Some("../bank-types".to_string()),
                    types: vec![],
                    states: vec![],
                    functions: vec![],
                }],
                topics: vec![],
            }),
            states: vec![],
            types: vec![],
        }
    }

    fn second_package_definition() -> PackageDefinition {
        PackageDefinition {
            api_version: DATAFLOW_STABLE_VERSION.to_string(),
            meta: Header {
                namespace: "example".to_string(),
                name: "bank".to_string(),
                version: "0.1.0".to_string(),
            },
            types: vec![
                MetadataType {
                    name: "bank-event".to_string(),
                    type_: SdfType::Object(SdfObject {
                        fields: vec![
                            ObjectField {
                                name: "name".to_string(),
                                type_: TypeRef {
                                    name: "string".to_string(),
                                },
                                optional: false,
                                serde_config: SerdeConfig {
                                    serialize: None,
                                    deserialize: None,
                                },
                            },
                            ObjectField {
                                name: "amount".to_string(),
                                type_: TypeRef {
                                    name: "float32".to_string(),
                                },
                                optional: false,
                                serde_config: SerdeConfig {
                                    serialize: None,
                                    deserialize: None,
                                },
                            },
                        ],
                    }),
                    origin: SdfTypeOrigin::Local,
                },
                MetadataType {
                    name: "bank-account".to_string(),
                    type_: SdfType::ArrowRow(SdfArrowRow {
                        columns: vec![
                            SdfArrowColumn {
                                name: "balance".to_string(),
                                type_: ArrowColumnKind::Float32,
                            },
                            SdfArrowColumn {
                                name: "name".to_string(),
                                type_: ArrowColumnKind::String,
                            },
                        ],
                    }),
                    origin: SdfTypeOrigin::Local,
                },
            ],
            states: vec![StateTyped {
                name: "account-balance".to_string(),
                type_: SdfKeyedState {
                    key: TypeRef {
                        name: "string".to_string(),
                    },
                    value: SdfKeyedStateValue::U32,
                },
            }],
            imports: vec![],
            functions: vec![],
            dev: None,
        }
    }

    #[test]
    fn test_dependency_resolver_build() {
        let imports = vec![PackageImport {
            metadata: Header {
                namespace: "example".to_string(),
                name: "bank-update".to_string(),
                version: "0.1.0".to_string(),
            },
            path: Some("test/bank-update".to_string()),
            types: vec![],
            states: vec![],
            functions: vec![],
        }];

        let package_configs = vec![first_package_definition(), second_package_definition()];

        let resolver = DependencyResolver::build(imports, package_configs, false).unwrap();
        let pkgs = resolver.packages().unwrap();

        assert_eq!(pkgs.len(), 2);

        let bank_update = pkgs
            .iter()
            .find(|pkg| pkg.meta.name == "bank-update")
            .unwrap();

        let bank_update_types = &bank_update.types;

        assert_eq!(
            bank_update_types.len(),
            3,
            "bank update should have 3 types imported from bank-types"
        );
        assert_eq!(bank_update_types.first().unwrap().name, "account-balance");
        assert_eq!(bank_update_types.get(1).unwrap().name, "bank-account");
        assert_eq!(bank_update_types.get(2).unwrap().name, "bank-event");

        let filter_positive = bank_update.functions.first().unwrap();
        assert_eq!(filter_positive.0.uses, "filter-position-events");
        let state_imported = filter_positive.0.states.first().unwrap();
        assert!(
            state_imported.is_resolved(),
            "state imported should be resolved by the dependency resolver"
        );
    }
}
