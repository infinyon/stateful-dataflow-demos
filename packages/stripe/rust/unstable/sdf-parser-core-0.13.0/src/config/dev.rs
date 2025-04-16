use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{SerdeConverter, TopicWrapper, import::PackageImport};

//Used for overriding elements in dataflow and packages for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DevConfig {
    pub converter: Option<SerdeConverter>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub imports: Vec<PackageImport>,
    #[serde(
        skip_serializing_if = "BTreeMap::is_empty",
        default,
        with = "serde_with::rust::maps_duplicate_key_is_error"
    )]
    pub topics: BTreeMap<String, TopicWrapper>,
}
