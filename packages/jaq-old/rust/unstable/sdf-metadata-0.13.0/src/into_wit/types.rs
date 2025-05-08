use anyhow::{anyhow, Context, Result};

use sdf_common::constants::VOID_TYPE_REPR;
use sdf_common::version::{ApiVersion, SdfContextVersion};
use sdf_parser_core::config::types::{
    CategoryKind, ColumnType, EnumType, KeyValueType, KeyedStateType, ListType, MetadataType,
    MetadataTypeInner, MetadataTypeTagged, MetadataTypesMap, NamedType, ObjectType, OptionType,
    RowType, SerdeFieldConfig, SerdeTypeConfig, EnumTaggingConfig,
};

use crate::into_wit::config::dataflow::MetadataType as MetadataTypeWit;
use crate::into_wit::config::io::TypeRef as TypeRefWit;
use crate::into_wit::config::metadata::{
    SdfType as SdfTypeWit, SdfObject as SdfObjectWit, ObjectField as ObjectFieldWit,
    SdfKeyedState as SdfKeyedStateWit, SdfEnum as SdfEnumWit, EnumField as EnumFieldWit,
    SdfList as SdfListWit, SdfOption as SdfOptionWit, SdfArrowRow as SdfArrowRowWit,
    SdfKeyedStateValue as SdfKeyedStateValueWit, SdfArrowColumn as SdfArrowColumnWit,
    ArrowColumnKind as ArrowColumnKindWit, SdfTypeOrigin as SdfTypeOriginWit,
    SdfKeyValue as SdfKeyValueWit, SerdeConfig as SerdeConfigWit,
    SerdeFieldConfig as SerdeFieldConfigWit, EnumTagging as EnumTaggingWit,
};

impl From<ColumnType> for ArrowColumnKindWit {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Typed(ty) => ty.into(),
        }
    }
}

impl From<RowType> for SdfArrowRowWit {
    fn from(wrapper: RowType) -> Self {
        SdfArrowRowWit {
            columns: wrapper
                .properties
                .into_iter()
                .map(|(name, ty)| SdfArrowColumnWit {
                    name,
                    type_: ty.into(),
                })
                .collect(),
        }
    }
}

impl From<CategoryKind> for ArrowColumnKindWit {
    fn from(wrapper: CategoryKind) -> Self {
        match wrapper {
            CategoryKind::U8 => Self::U8,
            CategoryKind::U16 => Self::U16,
            CategoryKind::U32 => Self::U32,
            CategoryKind::U64 => Self::U64,
            CategoryKind::S8 => Self::S8,
            CategoryKind::S16 => Self::S16,
            CategoryKind::S32 => Self::S32,
            CategoryKind::S64 => Self::S64,
            CategoryKind::Float32 => Self::Float32,
            CategoryKind::Float64 => Self::Float64,
            CategoryKind::Bool => Self::Bool,
            CategoryKind::String => Self::String,
            CategoryKind::Timestamp => Self::Timestamp,
        }
    }
}

pub(crate) trait IntoBinding {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>>;
}

impl IntoBinding for MetadataType {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let name = if let Some(ref type_name) = self.type_name {
            if !name.is_empty() {
                return Err(anyhow!("top level type should not have a type_name"));
            }
            type_name.to_owned()
        } else if name.is_empty() {
            let list_ty = self.ty.list_gen_name();

            if let Some(list_ty_name) = list_ty {
                list_ty_name.to_owned()
            } else {
                name.to_owned()
            }
        } else {
            name.to_owned()
        };

        if name.is_empty() {
            return Err(anyhow!("nested type requires a type-name"));
        }
        let ty = self.ty.into_bindings(&name, types, api_version)?;

        Ok(ty)
    }
}

impl IntoBinding for MetadataTypeInner {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let ty = match self {
            MetadataTypeInner::MetadataTypeTagged(ty) => {
                ty.into_bindings(name, types, api_version)?
            }
            MetadataTypeInner::None(_) => vec![MetadataTypeWit {
                name: name.to_owned(),
                type_: SdfTypeWit::Null,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeInner::NamedType(ty) => vec![ty.into_binding(name, api_version)],
        };
        Ok(ty)
    }
}

impl SdfKeyedStateValueWit {
    fn try_from_wrapper(wrapper: MetadataType, types: &MetadataTypesMap) -> Result<Self> {
        match wrapper.ty {
            MetadataTypeInner::MetadataTypeTagged(tagged) => tagged.try_into(),
            MetadataTypeInner::NamedType(named) => {
                if let Some(ty) = types.inner_type(&named.ty) {
                    match ty.ty {
                        MetadataTypeInner::MetadataTypeTagged(tagged) => tagged.try_into(),
                        _ => Err(anyhow!("unsupported type for value in keyed state")),
                    }
                } else {
                    Ok(SdfKeyedStateValueWit::Unresolved(TypeRefWit {
                        name: named.ty,
                    }))
                }
            }
            _ => Err(anyhow!("unsupported type for value in keyed state")),
        }
    }
}

impl IntoBinding for MetadataTypeTagged {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let name = name.to_owned();
        let ty = match self {
            MetadataTypeTagged::U8 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::U8,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::U16 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::U16,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::U32 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::U32,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::U64 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::U64,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::S8 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::S8,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::S16 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::S16,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::S32 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::S32,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::S64 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::S64,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::Float32 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::Float32,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::Float64 => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::Float64,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::Bool => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::Bool,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::String => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::String,
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::Bytes => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::Bytes,
                origin: SdfTypeOriginWit::Local,
            }],
            // update all from here
            MetadataTypeTagged::Object(obj) => {
                obj.into_bindings(name.as_str(), types, api_version)?
            }
            MetadataTypeTagged::Enum(e) => e.into_bindings(name.as_str(), types, api_version)?,
            MetadataTypeTagged::List(l) => l.into_bindings(name.as_str(), types, api_version)?,
            MetadataTypeTagged::Option(o) => o.into_bindings(name.as_str(), types, api_version)?,
            MetadataTypeTagged::KeyedState(kv) => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::KeyedState(SdfKeyedStateWit::try_from_wrapper(kv, types)?),
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::ArrowRow(r) => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::ArrowRow(r.into()),
                origin: SdfTypeOriginWit::Local,
            }],
            MetadataTypeTagged::KeyValue(kv) => vec![MetadataTypeWit {
                name,
                type_: SdfTypeWit::KeyValue(SdfKeyValueWit::try_from_wrapper(kv, types)?),
                origin: SdfTypeOriginWit::Local,
            }],
        };

        Ok(ty)
    }
}

impl IntoBinding for ObjectType {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let mut obj_types = vec![MetadataTypeWit {
            name: name.to_owned(),
            type_: SdfTypeWit::Object(self.clone().into_binding(name, api_version)),
            origin: SdfTypeOriginWit::Local,
        }];

        for (prop_name, prop) in self.properties.iter() {
            if !prop.ty.is_composite_type() {
                continue;
            }

            let prop_types = prop
                .ty
                .clone()
                .into_bindings("", types, api_version)
                .with_context(|| {
                    format!("cannot get nested types from {prop_name} property of {name} object")
                })?;
            obj_types.extend(prop_types);
        }

        Ok(obj_types)
    }
}

impl IntoBinding for EnumType {
    fn into_bindings(
        self,
        name: &str,
        _types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let mut types = vec![MetadataTypeWit {
            name: name.to_owned(),
            type_: self.clone().into_binding(name, api_version),
            origin: SdfTypeOriginWit::Local,
        }];

        for (variant_name, prop) in self.one_of.iter() {
            if let Some(ref ty) = prop.ty {
                if ty.is_composite_type() {
                    let prop_types = ty.clone().into_bindings("", _types, api_version).with_context(|| {
                        format!("cannot get nested types from variant {variant_name} from {name} enum type")
                    })?;
                    types.extend(prop_types);
                }
            }
        }
        Ok(types)
    }
}

impl IntoBinding for ListType {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let mut list_types = vec![MetadataTypeWit {
            name: name.to_owned(),
            type_: self.clone().into(),
            origin: SdfTypeOriginWit::Local,
        }];

        if self.items.is_composite_type() {
            let item_types = self
                .items
                .into_bindings("", types, api_version)
                .with_context(|| format!("cannot get nested types from {name} list type"))?;

            list_types.extend(item_types);
        }

        Ok(list_types)
    }
}

impl IntoBinding for OptionType {
    fn into_bindings(
        self,
        name: &str,
        types: &MetadataTypesMap,
        api_version: &ApiVersion,
    ) -> Result<Vec<MetadataTypeWit>> {
        let mut option_types = vec![MetadataTypeWit {
            name: name.to_owned(),
            type_: self.clone().into(),
            origin: SdfTypeOriginWit::Local,
        }];

        if self.value.is_composite_type() {
            let value_types = self
                .value
                .into_bindings("", types, api_version)
                .with_context(|| format!("cannot get nested types from {name} option type"))?;

            option_types.extend(value_types);
        }

        Ok(option_types)
    }
}

impl TryFrom<MetadataTypeTagged> for SdfKeyedStateValueWit {
    type Error = anyhow::Error;
    fn try_from(wrapper: MetadataTypeTagged) -> Result<Self> {
        match wrapper {
            MetadataTypeTagged::U32 => Ok(Self::U32),
            MetadataTypeTagged::ArrowRow(r) => Ok(Self::ArrowRow(r.into())),
            _ => Err(anyhow!("unsupported type for value in keyed state")),
        }
    }
}

pub(crate) trait IntoBinding2 {
    type Target;
    fn into_binding(self, name: &str, api_version: &ApiVersion) -> Self::Target;
}

impl IntoBinding2 for NamedType {
    type Target = MetadataTypeWit;
    fn into_binding(self, name: &str, _api_version: &ApiVersion) -> Self::Target {
        MetadataTypeWit {
            name: name.to_owned(),
            type_: SdfTypeWit::Named(TypeRefWit { name: self.ty }),
            origin: SdfTypeOriginWit::Local,
        }
    }
}

impl SdfKeyValueWit {
    pub(crate) fn try_from_wrapper(
        wrapper: KeyValueType,
        _types: &MetadataTypesMap,
    ) -> Result<Self> {
        let ty = SdfKeyValueWit {
            key: TypeRefWit {
                name: wrapper.properties.key.ty().into(),
            },
            value: TypeRefWit {
                name: wrapper.properties.value.ty().into(),
            },
        };
        Ok(ty)
    }
}
impl SdfKeyedStateWit {
    pub(crate) fn try_from_wrapper(
        wrapper: KeyedStateType,
        types: &MetadataTypesMap,
    ) -> Result<Self> {
        let ty = SdfKeyedStateWit {
            key: TypeRefWit {
                name: wrapper.properties.key.ty().into(),
            },
            value: SdfKeyedStateValueWit::try_from_wrapper(*wrapper.properties.value, types)?,
        };
        Ok(ty)
    }
}

impl IntoBinding2 for ObjectType {
    type Target = SdfObjectWit;
    fn into_binding(self, _name: &str, api_version: &ApiVersion) -> Self::Target {
        SdfObjectWit {
            fields: self
                .properties
                .into_iter()
                .map(|(name, prop)| {
                    let serde_config = prop.serde.clone().into_binding(&name, api_version);
                    ObjectFieldWit {
                        name,
                        type_: prop.ty.into(),
                        serde_config,
                        optional: prop.optional,
                    }
                })
                .collect(),
        }
    }
}

impl IntoBinding2 for SerdeTypeConfig {
    type Target = SerdeConfigWit;
    fn into_binding(self, name: &str, api_version: &ApiVersion) -> Self::Target {
        SerdeConfigWit {
            serialize: self
                .serialize
                .map(|s| s.into_binding(name, api_version))
                .or({
                    let rename = if api_version.is_v5() {
                        None
                    } else {
                        Some(name.to_owned())
                    };

                    Some(SerdeFieldConfigWit { rename })
                }),
            deserialize: self
                .deserialize
                .map(|d| d.into_binding(name, api_version))
                .or({
                    let rename = if api_version.is_v5() {
                        None
                    } else {
                        Some(name.to_owned())
                    };

                    Some(SerdeFieldConfigWit { rename })
                }),
        }
    }
}

impl IntoBinding2 for SerdeFieldConfig {
    type Target = SerdeFieldConfigWit;
    fn into_binding(self, name: &str, api_version: &ApiVersion) -> Self::Target {
        let rename = if api_version.is_v5() {
            self.rename
        } else {
            self.rename.or(Some(name.to_owned()))
        };
        SerdeFieldConfigWit { rename }
    }
}

impl IntoBinding2 for EnumType {
    type Target = SdfTypeWit;
    fn into_binding(self, _name: &str, api_version: &ApiVersion) -> Self::Target {
        let tagging = match self.tagging {
            EnumTaggingConfig::Untagged => Some(EnumTaggingWit::Untagged),
            _ => None,
        };
        SdfTypeWit::Enum(SdfEnumWit {
            variants: self
                .one_of
                .iter()
                .map(|(name, variant)| EnumFieldWit {
                    name: name.to_owned(),
                    value: if variant.ty() == VOID_TYPE_REPR {
                        None
                    } else {
                        Some(TypeRefWit {
                            name: variant.ty().to_owned(),
                        })
                    },
                    serde_config: variant.serde.clone().into_binding(name, api_version),
                })
                .collect(),
            tagging,
        })
    }
}

impl From<ListType> for SdfTypeWit {
    fn from(wrapper: ListType) -> Self {
        Self::List(SdfListWit {
            item: TypeRefWit {
                name: wrapper.items.ty().into(),
            },
        })
    }
}

impl From<OptionType> for SdfTypeWit {
    fn from(wrapper: OptionType) -> Self {
        Self::Option(SdfOptionWit {
            value: TypeRefWit {
                name: wrapper.value.ty().into(),
            },
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use sdf_common::version::ApiVersion;
    use serde::Deserialize;

    use sdf_parser_core::config::types::{
        EnumType, EnumVariantType, MetadataType, MetadataTypeInner, MetadataTypeTagged,
        MetadataTypesMap,
    };

    use crate::{
        into_wit::{config::metadata::SdfType as SdfTypeWit, types::IntoBinding2, IntoBinding},
        wit::metadata::{SerdeConfig, SerdeFieldConfig},
    };

    #[test]
    fn test_convert_of_enum_type() {
        let enum_type = EnumType {
            one_of: vec![
                (
                    "A".to_string(),
                    EnumVariantType {
                        ty: Some(
                            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U32).into(),
                        ),
                        serde: Default::default(),
                    },
                ),
                (
                    "B".to_string(),
                    EnumVariantType {
                        ty: Some(
                            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::S32).into(),
                        ),
                        serde: Default::default(),
                    },
                ),
                (
                    "C".to_string(),
                    EnumVariantType {
                        ty: None,
                        serde: Default::default(),
                    },
                ),
            ]
            .into_iter()
            .collect(),
            tagging: Default::default(),
        };
        let enum_type_wit: SdfTypeWit = enum_type.into_binding("my-enum", &ApiVersion::V5);

        let expected = SdfTypeWit::Enum(crate::into_wit::config::metadata::SdfEnum {
            tagging: Default::default(),
            variants: vec![
                crate::into_wit::config::metadata::EnumField {
                    name: "A".to_string(),
                    value: Some(crate::into_wit::config::io::TypeRef {
                        name: "u32".to_string(),
                    }),
                    serde_config: SerdeConfig {
                        serialize: Some(SerdeFieldConfig { rename: None }),
                        deserialize: Some(SerdeFieldConfig { rename: None }),
                    },
                },
                crate::into_wit::config::metadata::EnumField {
                    name: "B".to_string(),
                    value: Some(crate::into_wit::config::io::TypeRef {
                        name: "s32".to_string(),
                    }),
                    serde_config: SerdeConfig {
                        serialize: Some(SerdeFieldConfig { rename: None }),
                        deserialize: Some(SerdeFieldConfig { rename: None }),
                    },
                },
                crate::into_wit::config::metadata::EnumField {
                    name: "C".to_string(),
                    value: None,
                    serde_config: SerdeConfig {
                        serialize: Some(SerdeFieldConfig { rename: None }),
                        deserialize: Some(SerdeFieldConfig { rename: None }),
                    },
                },
            ],
        });

        assert_eq!(enum_type_wit, expected);
    }

    #[test]
    fn test_convert_of_nested_types_missing_type_name() {
        #[derive(Deserialize)]
        struct MyTypes {
            types: BTreeMap<String, MetadataType>,
        }
        let types_yaml = "
types:
  my-nested-enum:
    type: enum
    oneOf:
      empty:
        type: null
      nested:
        type: object
        properties:
          name:
            type: string
          my-list:
            type: list
            items:
              type: object
              properties:
                name:
                  type: string
                count:
                  type: s32
        ";
        let types: MyTypes = serde_yaml::from_str(types_yaml).unwrap();

        let my_enum = types.types.get("my-nested-enum").unwrap();

        let types = MetadataTypesMap::default();

        let my_enum_wit = my_enum
            .clone()
            .into_bindings("my-nested-enum", &types, &ApiVersion::V5)
            .expect_err("expected error");

        assert_eq!(
            my_enum_wit.to_string(),
            "cannot get nested types from variant nested from my-nested-enum enum type".to_string()
        );
    }

    #[test]
    fn test_convert_nested_types() {
        #[derive(Deserialize)]
        struct MyTypes {
            types: BTreeMap<String, MetadataType>,
        }
        let types_yaml = "
types:
  my-nested-enum:
    type: enum
    oneOf:
      empty:
        type: null
      nested:
        type: object
        type-name: nested-obj
        properties:
          name:
            type: string
          my-list:
            type: list
            type-name: my-list
            items:
              type: object
              type-name: my-obj
              properties:
                name:
                  type: string
                count:
                  type: s32
        ";
        let types: MyTypes = serde_yaml::from_str(types_yaml).unwrap();

        let my_enum = types.types.get("my-nested-enum").unwrap();

        let types = MetadataTypesMap::default();

        let my_enum_wit = my_enum
            .clone()
            .into_bindings("my-nested-enum", &types, &ApiVersion::V5)
            .expect("expected success");

        assert_eq!(my_enum_wit.len(), 4);
        assert_eq!(&my_enum_wit[0].name, "my-nested-enum");
        assert_eq!(&my_enum_wit[1].name, "nested-obj");
        assert_eq!(&my_enum_wit[2].name, "my-list");
        assert_eq!(&my_enum_wit[3].name, "my-obj");
    }

    #[test]
    fn test_convert_nested_list() {
        #[derive(Deserialize)]
        struct MyTypes {
            types: BTreeMap<String, MetadataType>,
        }
        let types_yaml = "
types:
    my-root-obj:
      type: object
      properties:
        name:
          type: string
        my-list:
          type: list
          items:
            type: object
            type-name: my-obj
            properties:
              name:
                type: string
              count:
                type: s32
        ";
        let types: MyTypes = serde_yaml::from_str(types_yaml).unwrap();

        let my_obj = types.types.get("my-root-obj").unwrap();

        let types = MetadataTypesMap::default();

        let my_obj_wit = my_obj
            .clone()
            .into_bindings("my-root-obj", &types, &ApiVersion::V5)
            .expect("expected success");

        assert_eq!(my_obj_wit.len(), 3);
        assert_eq!(&my_obj_wit[0].name, "my-root-obj");
        assert_eq!(&my_obj_wit[1].name, "list-my-obj-gen-type");
        assert_eq!(&my_obj_wit[2].name, "my-obj");
    }
}
