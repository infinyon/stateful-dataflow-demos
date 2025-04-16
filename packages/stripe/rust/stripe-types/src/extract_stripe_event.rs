use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::stripe_types_types::types::StripeEvent;
use crate::bindings::examples::stripe_types_types::types::Bytes;
#[allow(unused_imports)]
use crate::bindings::examples::stripe_types_types::types::*;
#[sdf(fn_name = "extract-stripe-event")]
pub(crate) fn extract_stripe_event(raw: Bytes) -> Result<StripeEvent> {
    println!("extract-stripe-event called - not implemented!");
    Err(sdfg::anyhow::anyhow!("extract-stripe-event is not implemented"))
}
