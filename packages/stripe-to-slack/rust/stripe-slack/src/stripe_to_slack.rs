use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::stripe_slack_types::types::StripeEvent;
use crate::bindings::examples::stripe_slack_types::types::SlackEvent;
#[allow(unused_imports)]
use crate::bindings::examples::stripe_slack_types::types::*;
#[sdf(fn_name = "stripe-to-slack")]
pub(crate) fn stripe_to_slack(se: StripeEvent) -> Result<SlackEvent> {
    println!("stripe-to-slack called - not implemented!");
    Err(sdfg::anyhow::anyhow!("stripe-to-slack is not implemented"))
}
