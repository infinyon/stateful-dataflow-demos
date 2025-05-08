include!(concat!(env!("OUT_DIR"), "/sdf_pkg_wit.rs"));

pub use wit_encoder;

#[cfg(feature = "host")]
pub mod package;
