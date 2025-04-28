use sdf_parser_core::config::import::{ImportMetadata, PackageImport, StateImport};

use crate::into_wit::config::package_interface::{
    FunctionImport as FunctionImportWit, PackageImport as PackageImportWit,
};
use crate::into_wit::config::operator::{StepState as StepStateWit, StateImport as StateImportWit};
use crate::into_wit::config::dataflow::Header as HeaderWit;

impl From<PackageImport> for PackageImportWit {
    fn from(import: PackageImport) -> Self {
        Self {
            metadata: import.package.into(),
            path: import.path.map(|p| p.to_string()),
            types: import.types.into_iter().map(|t| t.name).collect(),
            states: import.states.into_iter().map(|s| s.name).collect(),
            functions: import
                .functions
                .into_iter()
                .map(|f| FunctionImportWit {
                    name: f.name,
                    alias: f.alias,
                })
                .collect(),
        }
    }
}

impl From<ImportMetadata> for HeaderWit {
    fn from(meta: ImportMetadata) -> Self {
        Self {
            name: meta.name,
            version: meta.version,
            namespace: meta.namespace,
        }
    }
}

impl From<StateImport> for StepStateWit {
    fn from(state: StateImport) -> Self {
        Self::Unresolved(StateImportWit { name: state.name })
    }
}
