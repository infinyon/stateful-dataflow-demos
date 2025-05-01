use std::path::PathBuf;

use anyhow::Result;

pub use wit_encoder;

#[cfg(feature = "host")]
pub mod package;

static WIT_DEPS: include_dir::Dir<'static> =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/wit/deps");

pub fn generate_common_wit_deps(path: &PathBuf) -> Result<()> {
    WIT_DEPS.extract(path)?;
    Ok(())
}
