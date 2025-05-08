use crate::wit::metadata::{OutputType, Parameter, TypeRef};

impl Parameter {
    pub fn is_bool(&self) -> bool {
        self.type_.value_type_name() == "bool"
    }
}

impl Default for Parameter {
    fn default() -> Self {
        Self {
            type_: OutputType::Ref(TypeRef {
                name: "string".to_string(),
            }),
            optional: false,
        }
    }
}
