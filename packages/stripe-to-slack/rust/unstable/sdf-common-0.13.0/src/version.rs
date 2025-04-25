use anyhow::Result;

use crate::constants::DF_VALUE_WIT_TYPE;

#[derive(Debug, Clone)]
pub struct ApiVersion(pub semver::Version);

impl ApiVersion {
    pub const V4: Self = ApiVersion(semver::Version::new(0, 4, 0));
    pub const V5: Self = ApiVersion(semver::Version::new(0, 5, 0));
    pub const V6: Self = ApiVersion(semver::Version::new(0, 6, 0));

    pub fn from(version: &str) -> Result<Self> {
        Ok(Self(semver::Version::parse(version)?))
    }

    pub fn table_wit_type(&self) -> &str {
        DF_VALUE_WIT_TYPE
    }
}
