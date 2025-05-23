use sdfg::Result;
use sdfg::sdf;
use crate::bindings::infinyon::stripe_types_types::types::StripeEvent;
#[allow(unused_imports)]
use crate::bindings::infinyon::stripe_types_types::types::*;
#[sdf(fn_name = "test-event")]
pub(crate) fn test_event(sev: StripeEvent) -> Result<StripeEvent> {
    Ok(sev)
}
