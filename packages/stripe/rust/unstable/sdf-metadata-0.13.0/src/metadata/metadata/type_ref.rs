use sdf_common::render::wit_name_case;
use wit_encoder::Type;

use crate::wit::metadata::TypeRef;

impl TypeRef {
    pub fn wit_type(&self) -> Type {
        match self.name.as_str() {
            "u8" => Type::U8,
            "u16" => Type::U16,
            "u32" => Type::U32,
            "u64" => Type::U64,
            "s8" | "i8" => Type::S8,
            "s16" | "i16" => Type::S16,
            "i32" | "s32" => Type::S32,
            "i64" | "s64" => Type::S64,
            "f32" => Type::F32,
            "f64" => Type::F64,
            "bool" => Type::Bool,
            "string" => Type::String,
            _ => Type::Named(wit_name_case(&self.name).into()),
        }
    }
}
