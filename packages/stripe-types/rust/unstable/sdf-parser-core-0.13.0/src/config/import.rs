use std::fmt;

use schemars::JsonSchema;
use serde::{
    de::{Unexpected, Visitor},
    Deserialize, Deserializer, Serialize,
};
use anyhow::Result;

use super::Metadata;

pub type PackageMetadata = Metadata;

#[derive(Serialize, Deserialize, Debug, Clone, Default, JsonSchema)]
pub struct PackageImport {
    #[serde(rename = "pkg")]
    pub package: ImportMetadata,
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub types: Vec<TypeImport>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub states: Vec<StateImport>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub functions: Vec<FunctionImport>,
}

#[derive(Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct ImportMetadata {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

impl<'de> Deserialize<'de> for ImportMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(PackageImportMetadataVisitor)
    }
}

struct PackageImportMetadataVisitor;

impl Visitor<'_> for PackageImportMetadataVisitor {
    type Value = ImportMetadata;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a string of the form `<namespace>/<name>@<version>`"
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if s.is_empty() {
            return Err(serde::de::Error::invalid_value(Unexpected::Str(s), &self));
        }

        //check there is one "/" with content before and after
        let split = s.split('/').collect::<Vec<&str>>();
        let namespace = split
            .first()
            .ok_or(serde::de::Error::invalid_value(Unexpected::Str(s), &self))?;
        let rest = split
            .get(1)
            .ok_or(serde::de::Error::invalid_value(Unexpected::Str(s), &self))?;

        if namespace.is_empty() || rest.is_empty() {
            return Err(serde::de::Error::invalid_value(Unexpected::Str(s), &self));
        }

        let split = rest.split('@').collect::<Vec<&str>>();
        let name = split
            .first()
            .ok_or(serde::de::Error::invalid_value(Unexpected::Str(s), &self))?;
        let version = split
            .get(1)
            .ok_or(serde::de::Error::invalid_value(Unexpected::Str(s), &self))?;

        if name.is_empty() || version.is_empty() {
            return Err(serde::de::Error::invalid_value(Unexpected::Str(s), &self));
        }

        Ok(ImportMetadata {
            namespace: namespace.to_string(),
            name: name.to_string(),
            version: version.to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct FunctionImport {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct TypeImport {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct StateImport {
    pub name: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_name_does_not_parse() {
        let yaml = "";

        let result: Result<ImportMetadata, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_name_without_forward_slash_does_not_parse() {
        let yaml = "example-ns\\my-fn@0.0.0";

        let result: Result<ImportMetadata, _> = serde_yaml::from_str(yaml);
        let error = result.unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid value: string \"example-ns\\\\my-fn@0.0.0\", expected a string of the form `<namespace>/<name>@<version>`"
        )
    }

    #[test]
    fn test_name_without_at_sign_does_not_parse() {
        let yaml = "example-ns/my-fna0.0.0";

        let result: Result<ImportMetadata, _> = serde_yaml::from_str(yaml);
        let error = result.unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid value: string \"example-ns/my-fna0.0.0\", expected a string of the form `<namespace>/<name>@<version>`"
        )
    }

    #[test]
    fn test_valid_name_parses() {
        let yaml = "example-ns/my-fn@0.0.0";

        let result: Result<ImportMetadata, _> = serde_yaml::from_str(yaml);
        assert!(result.is_ok());
    }
}
