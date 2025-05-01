use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    ops::Deref,
};

use wit_encoder::{Interface, TypeDef, TypeDefKind, Use};

use sdf_common::version::ApiVersion;

use crate::wit::metadata::{SdfKeyedStateValue, SdfType, SdfTypeOrigin};

// native types that don't need to be imported in the wit bindings
const NATIVE_WIT_TYPES: [&str; 19] = [
    "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "s8", "s16", "s32", "s64", "f32", "f64",
    "bool", "f32", "f64", "string", "bytes",
];

pub fn is_imported_type(value: &str) -> bool {
    !is_sdf_native_type(value) || value == "bytes"
}

pub fn is_sdf_native_type(value: &str) -> bool {
    NATIVE_WIT_TYPES.contains(&value) || value.is_empty()
}

pub fn is_numeric_type(value: &str) -> bool {
    [
        "u8", "u16", "u32", "u64", "s8", "s16", "s32", "s64", "float32", "float64", "f64", "f32",
    ]
    .contains(&value)
}

type SdfTypeDefinition = (SdfType, SdfTypeOrigin);
#[derive(Default, Debug)]
pub struct SdfTypesMap {
    pub(crate) map: BTreeMap<String, SdfTypeDefinition>,
}

impl Deref for SdfTypesMap {
    type Target = BTreeMap<String, SdfTypeDefinition>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl SdfTypesMap {
    /// from a given type name, get a list of all the types required for a package importing
    /// it to be able to use it. This is:
    /// - the type itself
    /// - all the types it aliases until it reaches a primitive type.
    /// - if it's a keyed state, the value type
    ///
    /// Note that for now, we don't need to include the object inner types, as they are
    /// not used in the engine
    pub fn get_type_tree(&self, ty: &str) -> HashMap<String, SdfType> {
        let mut tree = HashMap::new();
        let mut current_ty = Cow::Borrowed(ty);
        while let Some(metadata_type) = self.get_type(current_ty.deref()) {
            match &metadata_type.0 {
                SdfType::Named(type_ref) => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    current_ty = type_ref.name.clone().into();
                }
                SdfType::KeyedState(keyed_state) => {
                    // for keyed state, we add the value types
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    if let SdfKeyedStateValue::Unresolved(r) = &keyed_state.value {
                        current_ty = r.name.clone().into();
                    } else {
                        break;
                    }
                }
                SdfType::Object(obj) => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    for prop in &obj.fields {
                        let prop_types = self.get_type_tree(&prop.type_.name);
                        for (name, ty) in prop_types {
                            tree.insert(name, ty);
                        }
                    }
                    break;
                }
                SdfType::Enum(en) => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    for variant in &en.variants {
                        if let Some(ty) = &variant.value {
                            let variant_types = self.get_type_tree(&ty.name);
                            for (name, ty) in variant_types {
                                tree.insert(name, ty);
                            }
                        }
                    }
                    break;
                }
                SdfType::List(l) => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    current_ty = l.item.name.clone().into();
                }
                SdfType::Option(o) => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    current_ty = o.value.name.clone().into();
                }
                _ => {
                    tree.insert(current_ty.to_string(), metadata_type.0.clone());

                    break;
                }
            }
        }
        tree
    }

    pub fn get_type(&self, ty: &str) -> Option<&(SdfType, SdfTypeOrigin)> {
        // also try replacing '-' with '_' in the key
        if let Some(ty) = self.map.get(ty) {
            return Some(ty);
        }
        if let Some(ty) = self.map.get(ty.replace('-', "_").as_str()) {
            return Some(ty);
        }

        None
    }

    pub fn inner_type_name(&self, ty_name: &str) -> Option<String> {
        match self.get_type(ty_name) {
            Some((SdfType::Named(type_ref), _)) => self.inner_type_name(&type_ref.name),
            Some(_) => Some(ty_name.to_owned()),
            None => {
                if NATIVE_WIT_TYPES.contains(&ty_name) {
                    Some(ty_name.to_owned())
                } else {
                    None
                }
            }
        }
    }

    pub fn resolve_alias(&self, ty: &str) -> String {
        match self.get_type(ty) {
            Some((sdf_type, _)) => {
                let resolved_name = match sdf_type {
                    SdfType::U8
                    | SdfType::U16
                    | SdfType::U32
                    | SdfType::U64
                    | SdfType::S8
                    | SdfType::S16
                    | SdfType::S32
                    | SdfType::S64
                    | SdfType::Float32
                    | SdfType::Float64
                    | SdfType::Bool
                    | SdfType::String => sdf_type.ty().to_owned(),
                    SdfType::Named(type_ref) => self.resolve_alias(&type_ref.name),
                    _ => ty.to_string(),
                };

                resolved_name
            }

            _ => ty.to_owned(),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        // also try replacing '-' with '_' in the key
        self.map.contains_key(key)
            | NATIVE_WIT_TYPES.contains(&key)
            | self.map.contains_key(key.replace('-', "_").as_str())
    }

    pub fn insert_local(&mut self, key: String, value: SdfType) -> Option<SdfTypeDefinition> {
        if NATIVE_WIT_TYPES.contains(&key.as_str()) {
            return None;
        }
        self.map.insert(key, (value, SdfTypeOrigin::Local))
    }

    pub fn insert_imported(&mut self, key: String, value: SdfType) -> Option<SdfTypeDefinition> {
        if NATIVE_WIT_TYPES.contains(&key.as_str()) {
            return None;
        }
        self.map.insert(key, (value, SdfTypeOrigin::Imported))
    }

    pub(crate) fn is_s64(&self, ty: &str) -> bool {
        if ty == "s64" || ty == "i64" {
            return true;
        }

        match self.get_type(ty) {
            Some((SdfType::Named(type_ref), _)) => self.is_s64(&type_ref.name),
            Some((SdfType::S64, _)) => true,
            _ => false,
        }
    }

    pub fn wit_interface(&self, api_version: &ApiVersion, imports: Vec<Use>) -> Interface {
        let mut interface = Interface::new("types");

        // type def: type bytes = list<u8>;
        let bytes_type_def = TypeDef::new(
            "bytes",
            wit_encoder::TypeDefKind::Type(wit_encoder::Type::List(Box::new(
                wit_encoder::Type::U8,
            ))),
        );
        interface.type_def(bytes_type_def);

        for import in imports {
            interface.use_(import);
        }

        let mut requires_df_value = false;

        for (name, (ty, _origin)) in &self.map {
            for wit_type_def in ty.wit_type_def(name, api_version) {
                if matches!(wit_type_def.kind(), TypeDefKind::Type(wit_encoder::Type::Named(name)) if name.to_string() == "df-value")
                {
                    requires_df_value = true;
                }
                interface.type_def(wit_type_def);
            }
        }

        if requires_df_value {
            let mut u = Use::new("sdf:df/lazy");
            u.item("df-value", None);
            interface.use_(u);
        }
        interface
    }

    pub fn has_df_value(&self) -> bool {
        self.map
            .values()
            .any(|(ty, _)| matches!(ty, SdfType::Named(named) if named.name == "df-value"))
    }
}

#[cfg(test)]
mod test {
    use sdf_common::display::WitInterfaceDisplay;

    use crate::wit::metadata::{
        ArrowColumnKind, ObjectField, SdfArrowColumn, SdfArrowRow, SdfKeyedState,
        SdfKeyedStateValue, SdfObject, SdfType, TypeRef, SerdeConfig,
    };
    use super::*;

    #[test]
    fn test_sdf_map_contains_native_types() {
        let map = SdfTypesMap::default();
        for ty in NATIVE_WIT_TYPES.iter() {
            assert!(map.contains_key(ty));
        }
    }

    #[test]
    fn test_inner_type() {
        let mut map = SdfTypesMap::default();
        map.insert_local(
            "sentence".into(),
            SdfType::Named(TypeRef {
                name: "string".into(),
            }),
        );

        let ty = map
            .inner_type_name("sentence")
            .expect("failed to unwrap inner type");
        assert_eq!(ty, "string");
    }

    #[test]
    fn test_get_type_tree() {
        let mut map = SdfTypesMap::default();
        map.insert_local(
            "sentence".into(),
            SdfType::Named(TypeRef {
                name: "string".into(),
            }),
        );
        map.insert_local("number".into(), SdfType::U32);
        map.insert_local(
            "words".into(),
            SdfType::Named(TypeRef {
                name: "sentence".into(),
            }),
        );

        map.insert_local(
            "word".into(),
            SdfType::Named(TypeRef {
                name: "sentence".into(),
            }),
        );

        map.insert_local(
            "my-state".into(),
            SdfType::KeyedState(SdfKeyedState {
                key: TypeRef {
                    name: "s32".to_string(),
                },
                value: SdfKeyedStateValue::ArrowRow(SdfArrowRow {
                    columns: vec![SdfArrowColumn {
                        name: "number".to_string(),
                        type_: ArrowColumnKind::S32,
                    }],
                }),
            }),
        );

        let tree = map.get_type_tree("my-state");

        assert_eq!(tree.len(), 1);
        assert!(tree.contains_key("my-state"));

        let tree = map.get_type_tree("word");
        assert_eq!(tree.len(), 2);
        assert!(tree.contains_key("word"));
        assert!(tree.contains_key("sentence"));
    }
    #[test]
    fn test_resolve_alias_resolves_aliased_types() {
        let mut map = SdfTypesMap::default();
        map.insert_local(
            "sentence".into(),
            SdfType::Named(TypeRef {
                name: "string".into(),
            }),
        );

        let name = map.resolve_alias("sentence");

        assert_eq!(name, "string".to_string());
    }

    #[test]
    fn test_resolve_alias_preserves_primitives() {
        let map = SdfTypesMap::default();

        let name = map.resolve_alias("string");
        assert_eq!(name, "string".to_string());
    }

    #[test]
    fn test_resolve_alias_preserves_datastructure_names() {
        let mut map = SdfTypesMap::default();
        let fields = vec![ObjectField {
            name: "balance".to_string(),
            type_: TypeRef {
                name: "f32".to_string(),
            },
            optional: false,
            serde_config: SerdeConfig {
                serialize: None,
                deserialize: None,
            },
        }];

        map.insert_local("bank-event".into(), SdfType::Object(SdfObject { fields }));

        let name = map.resolve_alias("bank-event");
        assert_eq!(name, "bank-event".to_string());
    }

    #[test]
    fn test_resolve_alias_resolves_repeatedly_renamed_types() {
        let mut map = SdfTypesMap::default();
        map.insert_local("sentence".into(), SdfType::String);
        map.insert_local(
            "paragraph".into(),
            SdfType::Named(TypeRef {
                name: "sentence".into(),
            }),
        );

        let name = map.resolve_alias("paragraph");
        assert_eq!(name, "string".to_string());
    }

    #[test]
    fn test_types_wit_interface() {
        let mut map = SdfTypesMap::default();
        map.insert_local("sentence".into(), SdfType::String);
        map.insert_local(
            "paragraph".into(),
            SdfType::Named(TypeRef {
                name: "sentence".into(),
            }),
        );
        map.insert_local(
            "my-record".into(),
            SdfType::Object(SdfObject {
                fields: vec![
                    ObjectField {
                        name: "name".to_string(),
                        type_: TypeRef {
                            name: "string".to_string(),
                        },
                        optional: false,
                        serde_config: SerdeConfig {
                            serialize: None,
                            deserialize: None,
                        },
                    },
                    ObjectField {
                        name: "age".to_string(),
                        type_: TypeRef {
                            name: "u8".to_string(),
                        },
                        optional: false,
                        serde_config: SerdeConfig {
                            serialize: None,
                            deserialize: None,
                        },
                    },
                ],
            }),
        );

        let wit_interface = map.wit_interface(
            &ApiVersion::from("0.5.0").expect("failed to init version"),
            vec![],
        );

        let wit_interface_str = WitInterfaceDisplay(wit_interface).to_string();

        let expected_wit_interface = r#"interface types {
  type bytes = list<u8>;
  record my-record {
    name: string,
    age: u8,
  }
  type paragraph = sentence;
  type sentence = string;
}
"#;

        assert_eq!(wit_interface_str, expected_wit_interface);
    }
}
