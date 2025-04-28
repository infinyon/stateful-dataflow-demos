use wit_encoder::TypeDef;

use crate::wit::metadata::SdfKeyedStateValue;

impl SdfKeyedStateValue {
    pub fn wit_type_def(&self, name: &str) -> TypeDef {
        let name = name.to_owned();
        match self {
            SdfKeyedStateValue::U32 => TypeDef::type_(name, wit_encoder::Type::U32),
            SdfKeyedStateValue::ArrowRow(row) => {
                TypeDef::new(name, wit_encoder::TypeDefKind::Record(row.wit_record()))
            }
            SdfKeyedStateValue::Unresolved(type_ref) => TypeDef::type_(name, type_ref.wit_type()),
        }
    }
}
