use serde::{Serialize, Deserialize};

use crate::config::import::StateImport;

use super::{NamedParameterWrapper, ParameterWrapper};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum StepInvocationDefinition {
    Code(Code),
    Function(FunctionDefinition),
}

impl StepInvocationDefinition {
    pub fn extra_deps(&self) -> Vec<Dependency> {
        match self {
            StepInvocationDefinition::Code(code) => code.dependencies.clone(),
            StepInvocationDefinition::Function(function) => function.dependencies.clone(),
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            StepInvocationDefinition::Code(_) => None,
            StepInvocationDefinition::Function(function) => Some(&function.uses),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Dependency {
    pub name: String,
    #[serde(flatten)]
    pub version: DependencyVersion,
    #[serde(default = "default_features")]
    pub default_features: bool,
    #[serde(default)]
    pub features: Vec<String>,
}

fn default_features() -> bool {
    true
}

impl Dependency {
    pub fn to_rust_dependency(&self) -> String {
        match &self.version {
            DependencyVersion::Version { version } => {
                if self.default_features && self.features.is_empty() {
                    format!("{} = \"{}\"", self.name, version)
                } else {
                    let mut dep = format!("{} = {{ version = \"{}\"", self.name, version);
                    if !self.default_features {
                        dep.push_str(", default-features = false");
                    }
                    if !self.features.is_empty() {
                        dep.push_str(&format!(
                            ", features = [\"{}\"]",
                            self.features.join("\", \"")
                        ));
                    }
                    dep.push_str(" }");
                    dep
                }
            }
            DependencyVersion::Path { path } => {
                let mut dep = format!("{} = {{ path = \"{}\"", self.name, path);
                if !self.default_features {
                    dep.push_str(", default-features = false");
                }

                if !self.features.is_empty() {
                    dep.push_str(&format!(
                        ", features = [\"{}\"]",
                        self.features.join("\", \"")
                    ));
                }
                dep.push_str(" }");
                dep
            }
            DependencyVersion::Git {
                git,
                branch,
                rev,
                tag,
            } => {
                let mut git = format!("{} = {{ git = \"{}\"", self.name, git);
                if let Some(branch) = branch {
                    git.push_str(&format!(", branch = \"{}\"", branch));
                }
                if let Some(rev) = rev {
                    git.push_str(&format!(", rev = \"{}\"", rev));
                }

                if let Some(tag) = tag {
                    git.push_str(&format!(", tag = \"{}\"", tag));
                }

                if !self.default_features {
                    git.push_str(", default-features = false");
                }

                if !self.features.is_empty() {
                    git.push_str(&format!(
                        ", features = [\"{}\"]",
                        self.features.join("\", \"")
                    ));
                }

                git.push_str(" }");
                git
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum DependencyVersion {
    Version {
        version: String,
    },
    Path {
        path: String,
    },
    Git {
        git: String,
        branch: Option<String>,
        rev: Option<String>,
        tag: Option<String>,
    },
}

/// Serialization representation of the code
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct Code {
    #[serde(alias = "$key$")]
    pub export_name: Option<String>, // used to validate pkg exports
    #[serde(default)]
    pub lang: Lang,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    #[serde(rename = "states")]
    pub state_imports: Vec<StateImport>,
    #[serde(default)]
    pub dependencies: Vec<Dependency>,
    pub run: String,
}

/// Supported lang in sdf for build and generation
#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Lang {
    #[default]
    Rust,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct FunctionDefinition {
    #[serde(alias = "$key$")]
    pub uses: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    #[serde(rename = "states")]
    pub state_imports: Vec<StateImport>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub inputs: Vec<NamedParameterWrapper>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub output: Option<ParameterWrapper>,
    #[serde(default)]
    pub lang: Lang,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub dependencies: Vec<Dependency>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_deserialize_code() {
        let yaml = r#"
lang: rust
run: |
    fn my_map(my_input: String) -> Result<String, String> {
        println!("Hello, world!");
    }
"#;
        let code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let code = match code {
            StepInvocationDefinition::Code(code) => code,
            _ => panic!("Invalid parsed code"),
        };

        assert_eq!(code.lang, Lang::Rust);
        assert!(code.state_imports.is_empty());
    }

    #[test]
    fn test_deserialize_code_with_input_key() {
        let yaml = r#"
lang: rust
run: |
    fn my_map(key: Option<String>, my_input: String) -> Result<String, String> {
        todo!()
    }
"#;
        let code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let code = match code {
            StepInvocationDefinition::Code(code) => code,
            _ => panic!("Invalid parsed code"),
        };

        assert_eq!(code.lang, Lang::Rust);
        assert!(code.state_imports.is_empty());
    }

    #[test]
    fn test_deserialize_code_with_output_key() {
        let yaml = r#"
lang: rust
run: |
    fn my_map(my_input: String) -> Result<(Option<i32>,String), String> {
        println!("Hello, world!");
    }
"#;
        let code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let code = match code {
            StepInvocationDefinition::Code(code) => code,
            _ => panic!("Invalid code"),
        };

        assert_eq!(code.lang, Lang::Rust);
        assert!(code.state_imports.is_empty());
    }

    #[test]
    fn test_deserialize_flat_map_code_with_output_key() {
        let yaml = r#"
        lang: rust
        run: |
            fn my_flatmap(my_input: String) -> Result<Option<(Option<String>,String)>, String> {
                println!("Hello, world!");
            }
        "#;

        let code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let code = match code {
            StepInvocationDefinition::Code(code) => code,
            _ => panic!("Invalid parsed code"),
        };

        assert_eq!(code.lang, Lang::Rust);
        assert!(code.state_imports.is_empty());
    }

    #[test]
    fn test_deserialize_function() {
        let yaml = r#"
lang: rust
uses: my-map
inputs:
  - name: my-input
    type: string
output:
    type: string
"#;

        let parsed_code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let function = match parsed_code {
            StepInvocationDefinition::Function(function) => function,
            _ => panic!("Invalid parsed code"),
        };
        assert_eq!(function.uses, "my-map");
        assert_eq!(function.inputs.len(), 1);
        assert_eq!(function.inputs[0].name, "my-input");
        assert_eq!(function.inputs[0].ty.ty(), "string");
        assert_eq!(function.output.unwrap().ty.ty(), "string");
    }

    #[test]
    fn test_deserialize_ambiguous_code_takes_priority() {
        let yaml = r#"
lang: rust
run: |
    fn my_map(my_input: String) -> Result<String, String> {
        println!("Hello, world!");
    }
uses: my-map
inputs:
  - name: my-input
    type: string
output:
    type: string
"#;
        let code: StepInvocationDefinition = serde_yaml::from_str(yaml).expect("parse yaml");

        let code = match code {
            StepInvocationDefinition::Code(code) => code,
            _ => panic!("Invalid parsed code"),
        };

        assert_eq!(code.lang, Lang::Rust);
        assert!(code.state_imports.is_empty());
        code.run.contains("Hello, world!");
    }
}
