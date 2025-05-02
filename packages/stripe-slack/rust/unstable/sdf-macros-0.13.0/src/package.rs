use std::path::PathBuf;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream, Result};
use syn::punctuated::Punctuated;
use syn::{token, LitStr, Token};

use sdf_parser_package::parse_package;
use sdf_wit::package::WitGenerator;

use crate::ast::create_ident;

pub struct PkgConfig {
    path: (Span, PathBuf),
    link_row: bool,
    link_df: bool,
}

impl Parse for PkgConfig {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let call_site = Span::call_site();

        let mut path = None;
        let mut link_row = false;
        let mut link_df = false;

        if input.peek(token::Brace) {
            let content;
            syn::braced!(content in input);
            let fields = Punctuated::<Opt, Token![,]>::parse_terminated(&content)?;
            for field in fields.into_pairs() {
                match field.into_value() {
                    Opt::Path(span, paths) => {
                        path = Some((span, PathBuf::from(paths.value())));
                    }
                    Opt::Row => {
                        link_row = true;
                    }
                    Opt::Df => {
                        link_df = true;
                    }
                }
            }
        }

        let path = path.ok_or_else(|| {
            syn::Error::new(
                call_site,
                "Missing path configuration. Try passing path in sdf_package!({ path: \"..\"});",
            )
        })?;

        Ok(PkgConfig {
            path,
            link_row,
            link_df,
        })
    }
}

impl PkgConfig {
    pub fn expand(self) -> Result<TokenStream> {
        let root: String = std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".into());
        let full_path = PathBuf::from(&root).join(&self.path.1);

        let file_content = std::fs::read_to_string(&full_path).map_err(|_e| {
            syn::Error::new(
                self.path.0,
                format!("{} file not found", self.path.1.display()),
            )
        })?;

        let dev_mode = std::env::var("SDF_PROD_MODE").is_err();

        let sdf_package = parse_package(&file_content).map_err(|e| {
            syn::Error::new(self.path.0, format!("{}: {}", self.path.1.display(), e))
        })?;

        let wit_inline = WitGenerator::builder()
            .dev_mode(dev_mode)
            .pkg_path(full_path)
            .build()
            .generate()
            .map_err(|e| {
                syn::Error::new(self.path.0, format!("Failed to generate wit files: {}", e))
            })?;

        let meta = &sdf_package.meta;

        let meta_name = &meta.name;
        let meta_version = &meta.version;
        let meta_namespace = &meta.namespace;
        let meta_name_ident = create_ident(meta_name);
        let meta_namespace_ident = create_ident(meta_namespace);

        let function_ident_names = sdf_package
            .functions
            .iter()
            .map(|f| match f.name() {
                Some(name) => Ok(create_ident(name)),
                None => Err(syn::Error::new(
                    self.path.0,
                    "not supported inline function in package",
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        let link_row = if self.link_row {
            Some(quote! {
                "sdf:row-state/row": ::sdfg::row_guest::bindings::sdf::row_state::row,
            })
        } else {
            None
        };
        let link_df = if self.link_df {
            Some(quote! {
                "sdf:df/lazy": ::sdfg::df_guest::wit::lazy,
            })
        } else {
            None
        };

        let modules_declaration = quote! {
            #(mod #function_ident_names;)*
        };

        let types_mod = create_ident(&format!("{}_types", meta_name));
        Ok(quote! {
            #modules_declaration
            pub mod pkg {
                pub mod metadata {
                    pub const NAME: &str = #meta_name;
                    pub const VERSION: &str = #meta_version;
                    pub const NAMESPACE: &str = #meta_namespace;
                }

                pub mod functions {
                    pub use super::bindings::exports::#meta_namespace_ident::#meta_name_ident::*;
                }

            pub mod bindings {
                ::sdfg::wit_bindgen::generate!({
                    // use out dir for generated files
                    inline: #wit_inline,
                    world: "default-world",
                    generate_all,
                    generate_unused_types: true,
                    runtime_path: "::sdfg::wit_bindgen::rt",
                    additional_derives: [::sdfg::serde::Serialize, ::sdfg::serde::Deserialize, PartialEq, Clone],
                    with: {
                      #link_row
                      #link_df
                    }
                });

                pub mod #meta_namespace_ident {
                    pub mod #meta_name_ident {
                        pub use crate::pkg::functions::types;
                    }
                    pub use self::#meta_name_ident as #types_mod;
                }
                pub struct Component;
              }
            }

            pub use pkg::bindings;
            pub use  pkg::bindings::Component;
            #[cfg(target_arch = "wasm32")]
            #[cfg(target_os = "wasi")]
            self::bindings::export!(Component with_types_in bindings);
        })
    }
}

enum Opt {
    Path(Span, syn::LitStr),
    Row,
    Df,
}

impl Parse for Opt {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let l = input.lookahead1();
        if l.peek(kw::path) {
            input.parse::<kw::path>()?;
            input.parse::<Token![:]>()?;
            let path: LitStr = input.parse()?;
            Ok(Opt::Path(path.span(), path))
        } else if l.peek(kw::row) {
            input.parse::<kw::row>()?;
            Ok(Opt::Row)
        } else if l.peek(kw::df) {
            input.parse::<kw::df>()?;
            Ok(Opt::Df)
        } else {
            Err(l.error())
        }
    }
}

mod kw {
    syn::custom_keyword!(path);
    syn::custom_keyword!(row);
    syn::custom_keyword!(df);
}
