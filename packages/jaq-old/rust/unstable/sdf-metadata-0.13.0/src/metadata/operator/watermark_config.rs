use crate::wit::operator::WatermarkConfig;

#[allow(clippy::derivable_impls)]
impl Default for WatermarkConfig {
    fn default() -> Self {
        WatermarkConfig {
            idleness: None,
            grace_period: None,
        }
    }
}
