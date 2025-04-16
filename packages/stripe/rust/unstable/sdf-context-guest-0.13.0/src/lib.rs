#[allow(dead_code)]
#[rustfmt::skip]
#[allow(clippy::all)]
pub mod bindings {
    use wit_bindgen::generate;

    cfg_if::cfg_if!(
        if #[cfg(all(feature = "row", feature = "table"))] {
            generate!({
                world: "context-world",
                path: "wit",
                additional_derives: [PartialEq, Clone],
                generate_all,
                with: {
                    "sdf:row-state/row": ::sdf_row_guest::bindings::sdf::row_state::row,
                    "sdf:df/lazy": ::sdf_df_guest::bindings::sdf::df::lazy,
                }
            });  
        } else if #[cfg(feature = "row")] {
            generate!({
                world: "context-world",
                path: "wit",
                additional_derives: [PartialEq, Clone],
                generate_all,
                with: {
                    "sdf:row-state/row": ::sdf_row_guest::bindings::sdf::row_state::row,
                }
            });
        } else if #[cfg(feature = "table")] {
            generate!({
                world: "context-world",
                path: "wit",
                additional_derives: [PartialEq, Clone],
                generate_all,
                with: {
                    "sdf:df/lazy": ::sdf_df_guest::bindings::sdf::df::lazy,
                }
            });
        } else {
            generate!({
                world: "context-world",
                path: "wit",
                additional_derives: [PartialEq, Clone],
                generate_all,
        });
    });
}

pub mod wit {
    pub use crate::bindings::sdf::context::types;
    pub use crate::bindings::sdf::context::operator_context;
    pub use crate::bindings::sdf::value_state;

    pub mod utils {
        pub use crate::bindings::sdf::context::operator_context::{
            key, window, get_row_value, get_value32, get_list32,
        };
        #[cfg(feature = "table")]
        pub fn sql(query: &str) -> anyhow::Result<sdf_df_guest::LazyDf> {
            match crate::bindings::sdf::context::operator_context::sql(query) {
                Ok(df) => Ok(df.into()),
                Err(e) => Err(anyhow::anyhow!("{}", e)),
            }
        }
    }
}
