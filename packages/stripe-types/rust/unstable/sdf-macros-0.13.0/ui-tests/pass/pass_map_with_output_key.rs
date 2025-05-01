use anyhow::Result;
use sdf_macros::sdf;

#[sdf(
    map,
    path = "../../../../guests/sdf-macro-test/wit/map-with-output-key",
    package = "map-with-output-key",
    namespace = "examples"
)]
fn map_fn(my_input: String) -> Result<(Option<String>, String)> {
    Ok((None, my_input.to_uppercase()))
}

fn main() -> Result<()> {
    Ok(())
}
