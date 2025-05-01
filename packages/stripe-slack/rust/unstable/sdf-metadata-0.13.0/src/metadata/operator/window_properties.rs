use crate::wit::operator::{WindowProperties, WindowKind};

impl WindowProperties {
    pub fn window_kind(&self) -> &WindowKind {
        &self.kind
    }

    pub fn offset(&self) -> u64 {
        match &self.kind {
            WindowKind::Tumbling(tumbling) => tumbling.offset,
            WindowKind::Sliding(sliding) => sliding.offset,
        }
    }

    pub fn grace_period(&self) -> u64 {
        self.watermark_config.grace_period.unwrap_or(0)
    }

    /// Returns the time interval for new windows
    pub fn new_window_interval(&self) -> u64 {
        match &self.kind {
            WindowKind::Tumbling(tumbling) => tumbling.duration,
            WindowKind::Sliding(sliding) => sliding.slide,
        }
    }
}
