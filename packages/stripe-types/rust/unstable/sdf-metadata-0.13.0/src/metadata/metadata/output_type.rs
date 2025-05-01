use crate::wit::{
    io::TypeRef,
    metadata::{OutputType, SdfKeyValue},
};

impl OutputType {
    pub fn value_type_name(&self) -> &str {
        match self {
            Self::Ref(r) | Self::KeyValue(SdfKeyValue { value: r, .. }) => &r.name,
        }
    }

    pub fn key_type_name(&self) -> Option<&str> {
        match self {
            Self::Ref(_) => None,
            Self::KeyValue(SdfKeyValue { key, .. }) => Some(&key.name),
        }
    }

    pub fn value_type(&self) -> &TypeRef {
        match self {
            Self::Ref(r) | Self::KeyValue(SdfKeyValue { value: r, .. }) => r,
        }
    }

    pub fn key_type(&self) -> Option<&TypeRef> {
        match self {
            Self::Ref(_) => None,
            Self::KeyValue(SdfKeyValue { key, .. }) => Some(key),
        }
    }

    pub fn wit_type(&self) -> wit_encoder::Type {
        match self {
            Self::Ref(r) => r.wit_type(),
            Self::KeyValue(SdfKeyValue { key, value }) => {
                let key = wit_encoder::Type::option(key.wit_type());
                let types = vec![key, value.wit_type()];
                wit_encoder::Type::tuple(types)
            }
        }
    }
}

impl From<TypeRef> for OutputType {
    fn from(r: TypeRef) -> Self {
        Self::Ref(r)
    }
}
