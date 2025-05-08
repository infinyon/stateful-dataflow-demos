use crate::wit::{
    dataflow::Header,
    package_interface::{FunctionImport, PackageImport},
};

impl PackageImport {
    pub fn builder(header: Header) -> PackageImportBuilder {
        PackageImportBuilder::new(header)
    }

    pub fn merge(&mut self, other: &PackageImport) {
        self.types.extend(other.types.iter().cloned());
        self.types.sort();
        self.types.dedup();

        self.states.extend(other.states.iter().cloned());
        self.states.sort();
        self.states.dedup();

        self.functions.extend(other.functions.iter().cloned());
        self.functions.sort_by(|a, b| a.name.cmp(&b.name));
        self.functions.dedup();
    }
}

#[derive(Default)]
pub struct PackageImportBuilder {
    header: Header,
    path: Option<String>,
    types: Vec<String>,
    states: Vec<String>,
    functions: Vec<FunctionImport>,
}

impl PackageImportBuilder {
    pub fn new(header: Header) -> Self {
        Self {
            header,
            ..Default::default()
        }
    }

    pub fn path(mut self, path: String) -> Self {
        self.path = Some(path);
        self
    }

    pub fn types(mut self, types: Vec<String>) -> Self {
        self.types = types;
        self
    }

    pub fn states(mut self, states: Vec<String>) -> Self {
        self.states = states;
        self
    }

    pub fn functions(mut self, functions: Vec<FunctionImport>) -> Self {
        self.functions = functions;
        self
    }

    pub fn build(self) -> PackageImport {
        PackageImport {
            metadata: self.header,
            path: self.path,
            types: self.types,
            states: self.states,
            functions: self.functions,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::wit::{
        metadata::Header,
        package_interface::{FunctionImport, PackageImport},
    };

    #[test]
    fn test_merge_adds_assets() {
        let mut existing_import = PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string()],
            states: vec!["my-state".to_string()],
            functions: vec![FunctionImport {
                name: "cat_map_cat".to_string(),
                alias: None,
            }],
        };

        let new_import = PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["dog".to_string()],
            states: vec!["my_other_state".to_string()],
            functions: vec![FunctionImport {
                name: "cat_map_dog".to_string(),
                alias: None,
            }],
        };

        existing_import.merge(&new_import);

        assert_eq!(
            existing_import.types,
            vec!["cat".to_string(), "dog".to_string()]
        );
        assert_eq!(
            existing_import.states,
            vec!["my-state".to_string(), "my_other_state".to_string()]
        );
        assert_eq!(
            existing_import.functions,
            vec![
                FunctionImport {
                    name: "cat_map_cat".to_string(),
                    alias: None,
                },
                FunctionImport {
                    name: "cat_map_dog".to_string(),
                    alias: None,
                },
            ]
        );
    }

    #[test]
    fn test_merge_adds_assets_without_duplication() {
        let mut existing_import = PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string()],
            states: vec!["my-state".to_string()],
            functions: vec![FunctionImport {
                name: "cat_map_cat".to_string(),
                alias: None,
            }],
        };

        let new_import = PackageImport {
            metadata: Header {
                name: "cats_package".to_string(),
                version: "0.1.0".to_string(),
                namespace: "inf-namespace".to_string(),
            },
            path: None,
            types: vec!["cat".to_string(), "dog".to_string()],
            states: vec!["my-state".to_string(), "my_other_state".to_string()],
            functions: vec![
                FunctionImport {
                    name: "cat_map_cat".to_string(),
                    alias: None,
                },
                FunctionImport {
                    name: "cat_map_dog".to_string(),
                    alias: None,
                },
            ],
        };

        existing_import.merge(&new_import);

        assert_eq!(
            existing_import.types,
            vec!["cat".to_string(), "dog".to_string()]
        );
        assert_eq!(
            existing_import.states,
            vec!["my-state".to_string(), "my_other_state".to_string()]
        );
        assert_eq!(
            existing_import.functions,
            vec![
                FunctionImport {
                    name: "cat_map_cat".to_string(),
                    alias: None,
                },
                FunctionImport {
                    name: "cat_map_dog".to_string(),
                    alias: None,
                },
            ]
        );
    }
}
