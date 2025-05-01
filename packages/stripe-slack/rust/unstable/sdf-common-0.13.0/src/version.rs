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

pub const COMMON_VERSION: &str = "0.13.0";

const V5: u64 = 5;
const V6: u64 = 6;

pub trait SdfContextVersion {
    fn is_stable(&self) -> bool;

    fn is_dev(&self) -> bool;

    fn is_v5(&self) -> bool;

    fn is_v6(&self) -> bool;

    fn crates_common_version(&self) -> &str;

    fn table_import(&self) -> &str;

    fn wit_bindgen_version(&self) -> &str;
}

impl SdfContextVersion for ApiVersion {
    fn is_stable(&self) -> bool {
        self.is_v6() || self.is_v5()
    }

    fn is_dev(&self) -> bool {
        true
    }

    fn is_v5(&self) -> bool {
        self.0.minor == V5
    }

    fn is_v6(&self) -> bool {
        self.0.minor == V6
    }

    fn crates_common_version(&self) -> &str {
        COMMON_VERSION
    }

    fn table_import(&self) -> &str {
        "sdf:df/lazy.{ df-value }"
    }

    fn wit_bindgen_version(&self) -> &str {
        "0.34.0"
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_version() {
        let stable_v5 = ApiVersion::from("0.5.0").expect("version");
        assert!(stable_v5.is_stable());
        assert!(!stable_v5.is_dev());
    }

    #[test]
    fn test_macros() {
        let stable_v5 = ApiVersion::from("0.5.0").expect("version");
        assert_eq!(stable_v5.crates_common_version(), "0.13.0");
    }
}
