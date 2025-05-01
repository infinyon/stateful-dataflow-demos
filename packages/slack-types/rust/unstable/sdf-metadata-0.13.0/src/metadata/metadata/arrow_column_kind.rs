use wit_encoder::Type;

use crate::wit::metadata::ArrowColumnKind;

impl ArrowColumnKind {
    pub fn rust_variant_str(&self) -> &'static str {
        match self {
            Self::U8 => "U8",
            Self::U16 => "U16",
            Self::U32 => "U32",
            Self::U64 => "U64",
            Self::S8 => "I8",
            Self::S16 => "I16",
            Self::S32 => "I32",
            Self::S64 => "I64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Bool => "Bool",
            Self::String => "String",
            Self::Timestamp => "Timestamp",
        }
    }

    pub fn ty(&self) -> &'static str {
        match self {
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::S8 => "s8",
            Self::S16 => "s16",
            Self::S32 => "s32",
            Self::S64 => "s64",
            Self::Float32 => "f32",
            Self::Float64 => "f64",
            Self::Bool => "bool",
            Self::String => "string",
            Self::Timestamp => "s64",
        }
    }

    pub fn wit_type(&self) -> Type {
        match self {
            Self::U8 => Type::U8,
            Self::U16 => Type::U16,
            Self::U32 => Type::U32,
            Self::U64 => Type::U64,
            Self::S8 => Type::S8,
            Self::S16 => Type::S16,
            Self::S32 => Type::S32,
            Self::S64 => Type::S64,
            Self::Float32 => Type::F32,
            Self::Float64 => Type::F64,
            Self::Bool => Type::Bool,
            Self::String => Type::String,
            Self::Timestamp => Type::S64,
        }
    }
}
