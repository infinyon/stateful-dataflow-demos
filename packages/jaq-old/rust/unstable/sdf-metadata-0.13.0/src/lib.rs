#[cfg(any(feature = "host", feature = "guest"))]
pub mod wit {
    pub use bindings::sdf::metadata::*;
    pub use bindings::exports::sdf::metadata as exports;

    cfg_if::cfg_if! {
      if #[cfg(feature = "host")] {
        mod bindings {
            wasmtime::component::bindgen!({
                path: "wit/sdf.wit",
                world: "dataflow-guest",
                async: false,
                trappable_imports: true,
                additional_derives: [serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq],
            });
        }
    } else {
        #[cfg(all(feature = "guest", not(feature = "host")))]
        #[allow(dead_code)]
        #[rustfmt::skip]
        #[allow(clippy::all)]
        mod bindings {
            use wit_bindgen::generate;

            generate!({
                world: "dataflow-guest",
                path: "wit",
                additional_derives: [serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq],
            });
        }
    }
    }
}

cfg_if::cfg_if! {
    if #[cfg(any(feature = "host", feature = "guest"))] {
        mod importer;
        pub mod util;
        pub mod metadata;

        #[cfg(feature = "yaml")]
        pub mod into_wit;
    }
}
