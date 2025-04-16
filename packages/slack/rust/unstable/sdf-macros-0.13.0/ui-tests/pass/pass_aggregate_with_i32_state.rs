use anyhow::Result;
use sdf_macros::sdf;

#[sdf(
    aggregate,
    path = "../../../../guests/sdf-macro-test/wit/aggregate-with-scalar-state",
    package = "aggregate-with-scalar-state",
    namespace = "examples",
    state = (
        name = "my-state",
        ty = i32
    )
)]
fn aggregate_fn() -> Result<Vec<String>> {
    let my_state = my_state();
    assert!(!my_state.is_empty());
    Ok(vec!["a".to_string(), "b".to_string()])
}

fn main() -> Result<()> {
    Ok(())
}
