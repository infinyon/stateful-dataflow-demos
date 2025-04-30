use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use anyhow::{anyhow, Result};

use crate::{
    util::config_error::{ConfigError, INDENT},
    wit::metadata::Header,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct HeaderValidationError {
    pub msg: String,
}

impl HeaderValidationError {
    pub fn new(msg: &str) -> Self {
        Self {
            msg: msg.to_string(),
        }
    }
}

impl ConfigError for HeaderValidationError {
    fn readable(&self, indents: usize) -> String {
        format!("{}{}", INDENT.repeat(indents), self.msg)
    }
}

impl Header {
    /// extract header from package name which is in the format namespace/name@version
    pub fn from_pkg_name(pkg_name: &str) -> Result<Self> {
        let parts: Vec<&str> = pkg_name.split('/').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "invalid package name format, it should be namespace/name@version"
            ));
        }

        let namespace = parts[0].to_string();
        let parts: Vec<&str> = parts[1].split('@').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "invalid package name format, it should be namespace/name@version"
            ));
        }

        let name = parts[0].to_string();
        let version = parts[1].to_string();
        Ok(Header {
            name,
            namespace,
            version,
        })
    }

    pub fn validate(&self) -> Result<(), Vec<HeaderValidationError>> {
        let mut errors = vec![];

        if self.name.is_empty() {
            errors.push(HeaderValidationError::new("Name cannot be empty\n"));
        }

        if self.namespace.is_empty() {
            errors.push(HeaderValidationError::new("Namespace cannot be empty\n"));
        }

        if self.version.is_empty() {
            errors.push(HeaderValidationError::new("Version cannot be empty\n"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// convert metadata to canonical name that can be used as file name
    pub fn canonical_name(&self) -> String {
        format!("{}__{}__{}", self.namespace, self.name, self.version)
    }
}

impl Display for Header {
    /// convert metadata to package name
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}@{}", self.namespace, self.name, self.version)
    }
}

/// parse header from canonical name
impl FromStr for Header {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("__").collect();
        if parts.len() != 3 {
            return Err(anyhow!(
                "invalid header format, it should be namespace__name__version"
            ));
        }

        let namespace = parts[0].to_string();
        let name = parts[1].to_string();
        let version = parts[2].to_string();
        Ok(Header {
            name,
            namespace,
            version,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn create_header() -> Header {
        Header {
            name: "my-df".to_string(),
            namespace: "example".to_string(),
            version: "1.0.0".to_string(),
        }
    }

    #[test]
    fn test_validate_rejects_invalid_name() {
        let metadata = Header {
            name: "".to_string(),
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
        };

        let res = metadata
            .validate()
            .expect_err("should error for empty name");

        assert!(res.contains(&HeaderValidationError::new("Name cannot be empty\n")));
    }

    #[test]
    fn test_validate_rejects_invalid_namespace() {
        let metadata = Header {
            name: "my-df".to_string(),
            namespace: "".to_string(),
            version: "1.0.0".to_string(),
        };

        let res = metadata
            .validate()
            .expect_err("should error for empty namspace");

        assert!(res.contains(&HeaderValidationError::new("Namespace cannot be empty\n")));
    }

    #[test]
    fn test_validate_rejects_invalid_version() {
        let metadata = Header {
            name: "my-df".to_string(),
            namespace: "example".to_string(),
            version: "".to_string(),
        };

        let res = metadata
            .validate()
            .expect_err("should error for empty version");

        assert!(res.contains(&HeaderValidationError::new("Version cannot be empty\n")));
    }

    #[test]
    fn test_validate_accepts_valid_metadata_config() {
        let metadata = Header {
            name: "my-df".to_string(),
            namespace: "example".to_string(),
            version: "0.0.0".to_string(),
        };

        metadata.validate().expect("should validate");
    }

    #[test]
    fn test_canonical_name() {
        let metadata = create_header();

        assert_eq!(metadata.canonical_name(), "example__my-df__1.0.0");
        assert!("example_my-df__0.0.0".parse::<Header>().is_err());
        let header = "example__my-df__1.1.0".parse::<Header>().expect("header");
        assert_eq!(header.name, "my-df");
        assert_eq!(header.namespace, "example");
        assert_eq!(header.version, "1.1.0");
    }

    #[test]
    fn test_header_print_pkg_name() {
        let metadata = create_header();
        assert_eq!(metadata.to_string(), "example/my-df@1.0.0");
    }

    #[test]
    fn test_header_from_pkg_name() {
        assert_eq!(
            Header::from_pkg_name("example/my-df@1.0.0").expect("pkg"),
            create_header()
        );

        assert!(Header::from_pkg_name("example/my-df").is_err());
        assert!(Header::from_pkg_name("example/my-df@1.0.0@1.0.0").is_err());
        assert!(Header::from_pkg_name("example/my-df@1.0.0@1.0.0").is_err());
        assert!(Header::from_pkg_name("my-df@1.0.0").is_err());
    }
}
