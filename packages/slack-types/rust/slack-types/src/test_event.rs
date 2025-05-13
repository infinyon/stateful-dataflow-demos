use sdfg::Result;
use sdfg::sdf;
use crate::bindings::infinyon::slack_types_types::types::SlackEvent;
#[allow(unused_imports)]
use crate::bindings::infinyon::slack_types_types::types::*;
#[sdf(fn_name = "test-event")]
pub(crate) fn test_event(sev: SlackEvent) -> Result<SlackEvent> {
    Ok(sev)
}
