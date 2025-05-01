use anyhow::Result;
use sdf_macros::sdf;

#[sdf(
    map,
    path = "../../../../guests/sdf-macro-test/wit/map-with-key",
    package = "map-with-key",
    namespace = "examples"
)]
fn map_fn(_key: Option<String>, my_input: String) -> Result<String> {
    Ok(my_input.to_uppercase())
}

fn main() -> Result<()> {
    Ok(())
}
