use proc_macro2::TokenStream;
use quote::quote;

use crate::ast::{
    create_ident, SdfBindgenConfig, SdfOpConfig, SdfOperatorFn, SdfOperatorKind, State, StateType,
};

pub(crate) fn generate_operator(
    func: &SdfOperatorFn,
    bindgen_config: &SdfBindgenConfig,
) -> TokenStream {
    let bindings = common_bindings_generate(bindgen_config);
    generate_trait_impl(func, bindings)
}

fn generate_trait_impl(func: &SdfOperatorFn, bindings: TokenStream) -> TokenStream {
    let f_name = &func.name;

    let input_signatures = func
        .input_types
        .iter()
        .enumerate()
        .map(|(idx, ty)| {
            let input_name = create_ident(&format!("input_{}", idx));
            quote! { #input_name: #ty, }
        })
        .collect::<Vec<_>>();

    let input_names = func
        .input_types
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            let input_name = create_ident(&format!("input_{}", idx));
            quote! { #input_name, }
        })
        .collect::<Vec<_>>();

    let output_type = &func.output_type;
    let func = &func.func;

    quote! {
        pub mod _sdf_gen_ {
          #bindings

          impl _GuestSdfInterface for Component {
              fn #f_name(#(#input_signatures)*) -> ::std::result::Result<#output_type, String> {
                  match super::#f_name(#(#input_names )*) {
                    Ok(output)  => Ok(output),
                    Err(err) => {
                      eprintln!("Error in {} operator: {}", stringify!(#f_name), err);
                      return Err(err.to_string());
                  }
                }
              }
          }
        }

        pub use _sdf_gen_::*;

        #func
    }
}

fn generate_state_consts(bindgen_config: &SdfBindgenConfig) -> TokenStream {
    let v: Vec<_> = bindgen_config
        .states
        .iter()
        .map(|s| generate_state_const(s, &bindgen_config.kind().unwrap_or_default()))
        .collect();

    quote! {
        #(#v)*
    }
}

fn generate_state_const(state_config: &State, op_type: &SdfOperatorKind) -> TokenStream {
    let state_const_name = state_config.const_name();
    let state_name = state_config.state_name();
    let state_name_str = state_config.state_name_str();

    match &state_config.ty {
        StateType::I32 => match op_type {
            SdfOperatorKind::Aggregate => {
                let rust_type = &state_config.type_name();
                quote! {
                    pub(crate) fn #state_name() -> crate::#rust_type {
                        match get_list32(#state_name_str.into()).map(|c| c.get()) {
                            Some(v) => v.into_iter().map(|(k, v)| (k, v as u32)).collect(),
                            None => {
                                eprintln!("Unexpected empty result fetching: {}", #state_name_str);
                                vec![]
                            }
                        }
                    }
                }
            }
            _ => {
                quote! {
                pub(crate) fn #state_name() -> ::sdfg::context_guest::wit::value_state::values::Value32 {
                    get_value32(#state_name_str.into()).expect("expected value32")
                    }
                }
            }
        },
        StateType::Row => {
            let state_config_wrapper_ty = state_config.wrapper_type();
            let item_value_type = state_config.item_value_type();

            let update_fn = if let Some(update_fn) = &state_config.update_fn {
                let update_fn = &update_fn.block.stmts;
                quote! {
                    pub fn update(&self) -> ::sdfg::Result<()> {
                        #(#update_fn;)*

                        Ok(())
                    }
                }
            } else {
                quote! {}
            };
            quote! {

                pub(crate) fn #state_name() -> #state_config_wrapper_ty {
                    let resource = get_row_value(#state_name_str.into()).expect("expected value");
                    #state_config_wrapper_ty::deserialize_from(resource).expect("deserialize")
                }

                pub struct #state_config_wrapper_ty {
                    _inner_value: super::#item_value_type,
                    resource: ::sdfg::row_guest::bindings::sdf::row_state::row::RowValue,
                }

                impl std::fmt::Debug for #state_config_wrapper_ty {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.debug_struct(stringify!(#state_config_wrapper_ty))
                            .field("value", &self._inner_value)
                            .finish()
                    }
                }

                impl std::ops::Deref for #state_config_wrapper_ty {
                    type Target = super::#item_value_type;
                    fn deref(&self) -> &Self::Target {
                        &self._inner_value
                    }
                }

                impl std::ops::DerefMut for #state_config_wrapper_ty {
                    fn deref_mut(&mut self) -> &mut Self::Target {
                        &mut self._inner_value
                    }
                }

                impl #state_config_wrapper_ty {
                    fn deserialize_from(row: ::sdfg::row_guest::bindings::sdf::row_state::row::RowValue) -> Result<Self, String> {
                        let _inner_value = ::sdfg::row_guest::RowSerde::deserialize_from(&row)
                            .map_err(|err| err.to_string())?;
                        Ok(Self { _inner_value, resource: row })
                    }

                   #update_fn
                }
            }
        }
        StateType::ListI32 => {
            quote! {
                static #state_const_name: std::sync::OnceLock<::sdfg::context_guest::wit::value_state::values::List32> = std::sync::OnceLock::new();
                pub(crate) fn #state_name() -> Vec<(String, i32)> {
                    #state_const_name.get_or_init(|| get_list32(#state_name_str.into()).expect("expected list32")).get()
                }
            }
        }
        StateType::Table => {
            quote! {
                static #state_const_name: std::sync::OnceLock<::sdfg::df_guest::LazyDf> = std::sync::OnceLock::new();
                pub(crate) fn #state_name() -> &'static ::sdfg::df_guest::LazyDf {
                    #state_const_name.get().expect("not initialized")
                }
            }
        }
    }
}
fn common_bindings_generate(bindgen_config: &SdfBindgenConfig) -> TokenStream {
    let state_consts = generate_state_consts(bindgen_config);
    let kind = bindgen_config.kind().unwrap_or_default();

    let row_binding = if bindgen_config
        .states
        .iter()
        .any(|state| matches!(state.ty, StateType::Row))
        & !matches!(kind, SdfOperatorKind::Aggregate)
    {
        Some(quote! {
            "sdf:row-state/row": ::sdfg::row_guest::bindings::sdf::row_state::row,
        })
    } else {
        None
    };

    let value_binding = if bindgen_config
        .states
        .iter()
        .any(|state| matches!(state.ty, StateType::I32))
        & !matches!(kind, SdfOperatorKind::Aggregate)
    {
        Some(quote! {
            "sdf:value-state/values": ::sdfg::context_guest::wit::value_state::values,
        })
    } else {
        None
    };

    let df_binding = bindgen_config
        .states
        .iter()
        .any(|state| matches!(state.ty, StateType::Table))
        || (matches!(kind, SdfOperatorKind::Aggregate)
            && bindgen_config
                .states
                .iter()
                .any(|state| matches!(state.ty, StateType::Row)));
    let table_binding = if df_binding {
        Some(quote! {
            "sdf:df/lazy": ::sdfg::df_guest::wit::lazy,
        })
    } else {
        None
    };

    let bindings = match &bindgen_config.config {
        SdfOpConfig::Pkg { fn_name } => {
            let interface_name = format!("{}-service", fn_name);
            let interface_ident = create_ident(&interface_name);
            quote! {
                pub(crate) use crate::pkg::bindings;
                pub use self::bindings::Component;

                pub(crate) use crate::pkg::functions::#interface_ident::*;
                use crate::pkg::functions::#interface_ident::Guest as _GuestSdfInterface;
                pub(crate) use ::sdfg::context_guest::wit::utils::*;
            }
        }
        SdfOpConfig::Config {
            package,
            path,
            world,
            namespace,
            interface,
            bindings_path,
            operator_kind: _,
        } => {
            let world_name = &world;
            let rust_namespace = create_ident(namespace);
            let rust_interface = create_ident(interface);
            let wit_path = &path;
            let rust_package = create_ident(package);

            let common_exports = quote! {
                pub use bindings::Component;
                pub(crate) use self::bindings::exports::#rust_namespace::#rust_package::#rust_interface::*;
                use self::bindings::exports::#rust_namespace::#rust_package::#rust_interface::Guest as _GuestSdfInterface;
                pub(crate) use ::sdfg::context_guest::wit::utils::*;
            };

            if let Some(bindings) = bindings_path {
                quote! {
                    pub(crate) use #bindings;
                    #common_exports
                }
            } else {
                quote! {
                    pub mod bindings {
                        use ::sdfg::wit_bindgen::generate;
                        generate!({
                            world : #world_name,
                            path : #wit_path,
                            runtime_path: "::sdfg::wit_bindgen::rt",
                            additional_derives:[::sdfg::serde::Serialize,::sdfg::serde::Deserialize],
                            generate_unused_types: true,
                            generate_all,
                            with: {
                                #row_binding
                                #table_binding
                                #value_binding
                            }
                        });
                        pub struct Component;
                    }
                    self::bindings::export!(Component with_types_in bindings);
                    #common_exports
                }
            }
        }
    };

    quote! {
        #[allow(dead_code)]
        #[allow(clippy::all)]
        #bindings

        #state_consts

    }
}
