mod ast;
mod generator;
mod package;

use proc_macro::TokenStream;
use syn::{parse_macro_input, spanned::Spanned, Error, Item, ItemFn};

use crate::ast::{SdfOperatorFn, SdfBindgenConfig};
use crate::package::PkgConfig;

#[proc_macro_attribute]
pub fn sdf(args: TokenStream, input: TokenStream) -> TokenStream {
    let sdf_bindgen_config = parse_macro_input!(args as SdfBindgenConfig);

    let input = parse_macro_input!(input as Item);

    match input {
        Item::Fn(item_fn) => sdf_operator_fn(sdf_bindgen_config, item_fn),
        _ => Error::new(input.span(), "macro supports only functions")
            .into_compile_error()
            .into(),
    }
}

fn sdf_operator_fn(bindgen_config: SdfBindgenConfig, func: ItemFn) -> TokenStream {
    let sdf_fn = match SdfOperatorFn::from_ast(&func, bindgen_config.kind()) {
        Ok(sdf_fn) => sdf_fn,
        Err(e) => return e.into_compile_error().into(),
    };

    generator::generate_operator(&sdf_fn, &bindgen_config).into()
}

#[proc_macro]
pub fn sdf_package(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    syn::parse_macro_input!(input as PkgConfig)
        .expand()
        .unwrap_or_else(Error::into_compile_error)
        .into()
}
