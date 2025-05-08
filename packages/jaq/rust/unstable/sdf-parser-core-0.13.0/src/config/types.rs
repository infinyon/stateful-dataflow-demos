use std::collections::HashMap;
use std::ops::DerefMut;
use std::{collections::BTreeMap, ops::Deref};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sdf_common::render::wit_name_case;
use sdf_common::constants::VOID_TYPE_REPR;

use crate::MaybeValid;

pub type CategoryKindWrapper = BTreeMap<String, Option<CategoryKind>>;

#[derive(Default, Debug, Clone)]
pub struct MetadataTypesMap {
    pub map: BTreeMap<String, MetadataType>,
}

impl MetadataTypesMap {
    /// For primitive types (MetadataTypeTagged), returns the type itself.
    /// For aliased types (MetadataType::NamedType), returns the original type
    ///
    /// For named data structures, returns the inner data structure, throwing out the name
    ///
    pub fn inner_type(&self, ty: &str) -> Option<MetadataType> {
        match self.map.get(ty).map(|ty| &ty.ty) {
            Some(MetadataTypeInner::NamedType(NamedType { ty })) => self.inner_type(ty),
            Some(ty) => Some(ty.to_owned().into()),
            None => MetadataTypeTagged::from_str(ty)
                .ok()
                .map(MetadataTypeInner::from)
                .map(MetadataType::from),
        }
    }
}

impl Deref for MetadataTypesMap {
    type Target = BTreeMap<String, MetadataType>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for MetadataTypesMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl From<BTreeMap<String, MetadataType>> for MetadataTypesMap {
    fn from(map: BTreeMap<String, MetadataType>) -> Self {
        Self { map }
    }
}

#[derive(Serialize, Clone, Deserialize, Default, Debug, PartialEq, Eq, JsonSchema)]
pub struct MetadataTypesMapWrapper {
    #[serde(flatten)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(deserialize_with = "::serde_with::rust::maps_duplicate_key_is_error::deserialize")]
    #[serde(serialize_with = "::serde_with::rust::maps_duplicate_key_is_error::serialize")]
    pub map: HashMap<String, MaybeValid<MetadataType>>,
}

impl TryFrom<MetadataTypesMapWrapper> for MetadataTypesMap {
    type Error = anyhow::Error;
    fn try_from(wrapper: MetadataTypesMapWrapper) -> Result<Self> {
        let mut map = BTreeMap::new();
        for (key, value) in wrapper.map {
            if let MaybeValid::Valid(ty) = value {
                map.insert(key, ty);
            } else {
                return Err(anyhow!("Invalid syntax for type with name: {}", key));
            }
        }
        Ok(MetadataTypesMap { map })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "kebab-case")]
pub enum CategoryKind {
    U8,
    U16,
    U32,
    U64,
    #[serde(alias = "i8")]
    S8,
    #[serde(alias = "i16")]
    S16,
    #[serde(alias = "i32")]
    S32,
    #[serde(alias = "i64")]
    S64,
    #[serde(alias = "f32")]
    Float32,
    #[serde(alias = "f64")]
    Float64,
    Bool,
    String,
    Timestamp,
}

impl CategoryKind {
    fn ty(&self) -> &'static str {
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
            Self::Timestamp => "string",
        }
    }

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
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnType {
    Typed(CategoryKind),
}

impl ColumnType {
    pub fn ty(&self) -> String {
        match self {
            Self::Typed(ty) => ty.ty().into(),
        }
    }

    pub fn rust_variant_str(&self) -> String {
        match self {
            Self::Typed(ty) => ty.rust_variant_str().into(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RowType {
    pub properties: BTreeMap<String, ColumnType>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct KeyValueType {
    pub properties: KeyValueProperties,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct KeyValueProperties {
    pub key: Box<MetadataType>,
    pub value: Box<MetadataType>,
}

impl From<KeyValueProperties> for KeyValueType {
    fn from(properties: KeyValueProperties) -> Self {
        Self { properties }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum MetadataTypeInner {
    MetadataTypeTagged(MetadataTypeTagged),
    NamedType(NamedType),
    None(NoneType),
}

impl MetadataTypeInner {
    pub fn list_gen_name(&self) -> Option<String> {
        if let MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::List(list)) = self {
            Some(format!("list-{}-gen-type", list.items.ty()))
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataType {
    #[serde(flatten)]
    pub ty: MetadataTypeInner,
    #[serde(alias = "type_name")]
    pub type_name: Option<String>,
}

impl FromStr for MetadataTypeInner {
    type Err = anyhow::Error;
    fn from_str(ty_str: &str) -> Result<Self> {
        let ty = match ty_str {
            "u8" => Self::MetadataTypeTagged(MetadataTypeTagged::U8),
            "u16" => Self::MetadataTypeTagged(MetadataTypeTagged::U16),
            "u32" => Self::MetadataTypeTagged(MetadataTypeTagged::U32),
            "u64" => Self::MetadataTypeTagged(MetadataTypeTagged::U64),
            "s8" | "i8" => Self::MetadataTypeTagged(MetadataTypeTagged::S8),
            "s16" | "i16" => Self::MetadataTypeTagged(MetadataTypeTagged::S16),
            "s32" | "i32" => Self::MetadataTypeTagged(MetadataTypeTagged::S32),
            "s64" | "i64" => Self::MetadataTypeTagged(MetadataTypeTagged::S64),
            "float32" | "f32" => Self::MetadataTypeTagged(MetadataTypeTagged::Float32),
            "float64" | "f64" => Self::MetadataTypeTagged(MetadataTypeTagged::Float64),
            "bool" => Self::MetadataTypeTagged(MetadataTypeTagged::Bool),
            "string" | "String" => Self::MetadataTypeTagged(MetadataTypeTagged::String),
            _ => Self::NamedType(NamedType {
                ty: wit_name_case(ty_str),
            }),
        };
        Ok(ty)
    }
}

impl FromStr for MetadataType {
    type Err = anyhow::Error;
    fn from_str(ty_str: &str) -> Result<Self> {
        let ty = MetadataTypeInner::from_str(ty_str)?;
        Ok(Self {
            ty,
            type_name: None,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default, JsonSchema)]
pub struct NoneType {
    #[serde(rename = "type")]
    type_: Option<()>,
}

impl MetadataType {
    pub fn ty(&self) -> &str {
        if let Some(type_name) = &self.type_name {
            return type_name;
        }
        match &self.ty {
            MetadataTypeInner::MetadataTypeTagged(ty) => ty.ty(),
            MetadataTypeInner::None(_) => VOID_TYPE_REPR,
            MetadataTypeInner::NamedType(ty) => &ty.ty,
        }
    }

    pub fn is_composite_type(&self) -> bool {
        matches!(
            self.ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Object(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Enum(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::List(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Option(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::ArrowRow(_))
                | MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyValue(_))
        )
    }
}

impl From<MetadataTypeTagged> for MetadataTypeInner {
    fn from(ty: MetadataTypeTagged) -> Self {
        Self::MetadataTypeTagged(ty)
    }
}

impl From<MetadataTypeTagged> for MetadataType {
    fn from(ty: MetadataTypeTagged) -> Self {
        let ty = MetadataTypeInner::MetadataTypeTagged(ty);
        Self {
            ty,
            type_name: None,
        }
    }
}

impl From<NamedType> for MetadataTypeInner {
    fn from(ty: NamedType) -> Self {
        Self::NamedType(ty)
    }
}

impl From<MetadataTypeInner> for MetadataType {
    fn from(ty: MetadataTypeInner) -> Self {
        Self {
            ty,
            type_name: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "kebab-case")]
pub enum MetadataTypeTagged {
    U8,
    U16,
    U32,
    U64,
    #[serde(alias = "i8")]
    S8,
    #[serde(alias = "i16")]
    S16,
    #[serde(alias = "i32")]
    S32,
    #[serde(alias = "i64")]
    S64,
    #[serde(alias = "f32")]
    Float32,
    #[serde(alias = "f64")]
    Float64,
    Bool,
    String,
    Bytes,
    Enum(EnumType),
    Object(ObjectType),
    List(ListType),
    Option(OptionType),
    KeyedState(KeyedStateType),
    ArrowRow(RowType),
    KeyValue(KeyValueType),
}

impl MetadataTypeTagged {
    pub fn ty(&self) -> &str {
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
            Self::Bytes => "bytes",
            Self::Enum(_) => "enum",
            Self::Object(_) => "object",
            Self::List(_) => "list",
            Self::Option(_) => "option",
            Self::KeyedState(_) => "keyed-state",
            Self::ArrowRow(_) => "arrow-row",
            Self::KeyValue(_) => "key-value",
        }
    }
}
impl FromStr for MetadataTypeTagged {
    type Err = anyhow::Error;
    fn from_str(ty_str: &str) -> Result<Self> {
        let ty = match ty_str {
            "u8" => Self::U8,
            "u16" => Self::U16,
            "u32" => Self::U32,
            "u64" => Self::U64,
            "s8" | "i8" => Self::S8,
            "s16" | "i16" => Self::S16,
            "s32" | "i32" => Self::S32,
            "s64" | "i64" => Self::S64,
            "float32" | "f32" => Self::Float32,
            "float64" | "f64" => Self::Float64,
            "bool" => Self::Bool,
            "string" | "String" => Self::String,
            _ => return Err(anyhow!("unsupported type")),
        };
        Ok(ty)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct NamedType {
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct KeyedStateType {
    pub properties: KeyedStateProperties,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct KeyedStateProperties {
    pub key: Box<MetadataType>,
    pub value: Box<MetadataType>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct ObjectType {
    pub properties: BTreeMap<String, ObjectPropertyType>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct ObjectPropertyType {
    #[serde(flatten)]
    pub ty: MetadataType,
    #[serde(default)]
    pub optional: bool,
    #[serde(flatten)]
    pub serde: SerdeTypeConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default, JsonSchema)]
pub struct SerdeTypeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deserialize: Option<SerdeFieldConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serialize: Option<SerdeFieldConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default, JsonSchema)]
pub struct SerdeFieldConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rename: Option<String>,
}

impl From<MetadataType> for ObjectPropertyType {
    fn from(ty: MetadataType) -> Self {
        Self {
            ty,
            optional: false,
            serde: Default::default(),
        }
    }
}

impl From<MetadataTypeInner> for ObjectPropertyType {
    fn from(ty: MetadataTypeInner) -> Self {
        Self {
            ty: ty.into(),
            optional: false,
            serde: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct EnumType {
    #[serde(rename = "oneOf")]
    pub one_of: BTreeMap<String, EnumVariantType>,
    #[serde(default)]
    pub tagging: EnumTaggingConfig,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum EnumTaggingConfig {
    #[default]
    ExternallyTagged,
    Untagged,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct EnumVariantType {
    #[serde(flatten)]
    pub ty: Option<MetadataType>,
    #[serde(flatten)]
    pub serde: SerdeTypeConfig,
}

impl EnumVariantType {
    pub fn ty(&self) -> &str {
        self.ty.as_ref().map(|ty| ty.ty()).unwrap_or(VOID_TYPE_REPR)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct ListType {
    #[serde(alias = "item")]
    pub items: Box<MetadataType>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct OptionType {
    pub value: Box<MetadataType>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_inner_type() {
        let mut map = MetadataTypesMap::default();
        map.insert(
            "sentence".into(),
            MetadataTypeInner::NamedType(NamedType {
                ty: "string".into(),
            })
            .into(),
        );

        let ty = map
            .inner_type("sentence")
            .expect("failed to unwrap inner type");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
        );
    }

    #[test]
    fn test_metadata_type_tagged_from_str() {
        let ty: MetadataTypeTagged = "u8".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::U8);

        let ty: MetadataTypeTagged = "u16".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::U16);

        let ty: MetadataTypeTagged = "u32".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::U32);

        let ty: MetadataTypeTagged = "u64".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::U64);

        let ty: MetadataTypeTagged = "s8".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S8);

        let ty: MetadataTypeTagged = "s16".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S16);

        let ty: MetadataTypeTagged = "s32".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S32);

        let ty: MetadataTypeTagged = "s64".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S64);

        let ty: MetadataTypeTagged = "i8".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S8);

        let ty: MetadataTypeTagged = "i16".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S16);

        let ty: MetadataTypeTagged = "i32".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S32);

        let ty: MetadataTypeTagged = "i64".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::S64);

        let ty: MetadataTypeTagged = "f32".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::Float32);

        let ty: MetadataTypeTagged = "f64".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::Float64);

        let ty: MetadataTypeTagged = "string".parse().expect("failed to parse");
        assert_eq!(ty, MetadataTypeTagged::String);

        "unknown-type"
            .parse::<MetadataTypeTagged>()
            .expect_err("failed to parse");
    }
    #[test]
    fn test_metadata_type_from_str() {
        use super::MetadataType;
        let ty: MetadataType = "u8".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U8).into()
        );

        let ty: MetadataType = "u16".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U16).into()
        );

        let ty: MetadataType = "u32".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U32).into()
        );

        let ty: MetadataType = "u64".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U64).into()
        );

        let ty: MetadataType = "s8".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S8).into()
        );

        let ty: MetadataType = "s16".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S16).into()
        );

        let ty: MetadataType = "s32".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S32).into()
        );

        let ty: MetadataType = "s64".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S64).into()
        );

        let ty: MetadataType = "i8".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S8).into()
        );

        let ty: MetadataType = "i16".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S16).into()
        );

        let ty: MetadataType = "i32".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S32).into()
        );

        let ty: MetadataType = "i64".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S64).into()
        );

        let ty: MetadataType = "f32".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Float32).into()
        );

        let ty: MetadataType = "f64".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Float64).into()
        );

        let ty: MetadataType = "f32".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Float32).into()
        );

        let ty: MetadataType = "f64".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Float64).into()
        );

        let ty: MetadataType = "bool".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Bool).into()
        );

        let ty: MetadataType = "string".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
        );

        let ty: MetadataType = "String".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String).into()
        );

        let ty: MetadataType = "my-type".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::NamedType(super::NamedType {
                ty: "my-type".into()
            })
            .into()
        );

        let ty: MetadataType = "MyType".parse().expect("failed to parse");
        assert_eq!(
            ty,
            MetadataTypeInner::NamedType(super::NamedType {
                ty: "my-type".into()
            })
            .into()
        );
    }

    #[test]
    fn test_type_name_overrides_ty() {
        let mut ty: MetadataType =
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U8).into();

        assert_eq!(ty.ty(), "u8");

        ty.type_name = Some("my-type".into());
        assert_eq!(ty.ty(), "my-type");
    }
    #[test]
    fn test_is_composite_type() {
        let ty: MetadataType = "u8".parse().expect("failed to parse");
        assert!(!ty.is_composite_type());

        let ty: MetadataType = "u8".parse().expect("failed to parse");
        assert!(!ty.is_composite_type());

        let ty: MetadataType = "string".parse().expect("failed to parse");
        assert!(!ty.is_composite_type());

        let ty: MetadataType = "my-type".parse().expect("failed to parse");
        assert!(!ty.is_composite_type());

        let ty: MetadataType =
            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Object(ObjectType {
                properties: Default::default(),
            }))
            .into();
        assert!(ty.is_composite_type());
    }
}
