use wit_encoder::Type;

use crate::wit::metadata::SdfKeyValue;

impl SdfKeyValue {
    pub fn wit_type(&self) -> Type {
        let types = vec![self.key.wit_type(), self.value.wit_type()];
        Type::tuple(types)
    }
}
