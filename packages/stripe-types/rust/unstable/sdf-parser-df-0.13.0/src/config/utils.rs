use anyhow::Result;
use humantime::Duration as HumanDuration;

pub const MAX_RESOURCE_NAME_LEN: usize = 63;

pub fn parse_to_millis(duration: &str) -> Result<u64> {
    let duration_ms = duration.parse::<HumanDuration>()?.as_millis() as u64;
    Ok(duration_ms)
}
