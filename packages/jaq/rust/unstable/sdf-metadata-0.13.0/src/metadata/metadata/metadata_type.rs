use wit_encoder::TypeDef;

use sdf_common::version::ApiVersion;

use crate::wit::metadata::{MetadataType, SdfTypeOrigin};

impl MetadataType {
    pub fn wit_type_def(&self, api_version: &ApiVersion) -> Vec<TypeDef> {
        self.type_.wit_type_def(&self.name, api_version)
    }
    pub fn is_imported(&self) -> bool {
        matches!(self.origin, SdfTypeOrigin::Imported)
    }
}
