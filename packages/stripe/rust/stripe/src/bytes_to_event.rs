use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::stripe_types::types::Bytes;
use crate::bindings::examples::stripe_types::types::StripeEvent;
#[allow(unused_imports)]
use crate::bindings::examples::stripe_types::types::*;

use serde_json;

#[sdf(fn_name = "bytes-to-event")]
pub(crate) fn bytes_to_event(raw: Bytes) -> Result<StripeEvent> {
    let event: StripeEvent = serde_json::from_slice(&raw)
        .map_err(|e| sdfg::anyhow::anyhow!("failed to parse StripeEvent JSON: {}", e))?;
    Ok(event)
}
