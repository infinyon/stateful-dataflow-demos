use std::path::Path;

use anyhow::{anyhow, Result};
use tracing::info;

use sdf_common::constants::DEFAULT_PACKAGE_FILE;
use sdf_metadata::wit::{
    dataflow::{DataflowDefinition, DevConfig, Header, PackageImport},
    package_interface::PackageDefinition,
};
use sdf_parser_host::host::HostParser;

pub fn load_package_dev_configs(
    dataflow: &mut DataflowDefinition,
    parser: &mut HostParser,
    pkg_dir: &Path,
) -> Result<()> {
    let package_configs = fetch_package_configs(
        pkg_dir,
        &mut dataflow.imports,
        dataflow.dev.as_ref(),
        true,
        parser,
    )?;

    info!(?package_configs, "loaded package dev configs");

    dataflow.packages = package_configs;

    Ok(())
}

pub fn load_package_imports(
    package: &mut PackageDefinition,
    parser: &mut HostParser,
    debug: bool,
    path: &Path,
) -> Result<Vec<PackageDefinition>> {
    fetch_package_configs(
        path,
        &mut package.imports,
        package.dev.as_ref(),
        debug,
        parser,
    )
}

// crawl the dependency tree to find package configs.
//
// will copy the `path` variable from the dev overrides
// into the import declarations as necessary, to find local imports when in dev mode
pub fn fetch_package_configs(
    pkg_dir: &Path,
    imports: &mut [PackageImport],
    dev_imports: Option<&DevConfig>,
    debug: bool,
    parser: &mut HostParser,
) -> Result<Vec<PackageDefinition>> {
    info!(pkg_dir=%pkg_dir.display(), "fetching package configs");

    let mut res = Vec::new();
    add_package_tree(&mut res, pkg_dir, imports, dev_imports, debug, parser)?;

    Ok(res)
}

fn add_package_tree(
    res: &mut Vec<PackageDefinition>,
    pkg_dir: &Path,
    imports: &mut [PackageImport],
    dev_imports: Option<&DevConfig>,
    debug: bool,
    parser: &mut HostParser,
) -> Result<()> {
    info!(pkg_dir=%pkg_dir.display(), "adding package tree");
    if debug {
        if let Some(dev_config) = dev_imports {
            info!(dev_config=?dev_config, "overriding with dev config");
            set_dev_overrides(imports, &dev_config.imports);
        }
    }

    for import in imports.iter() {
        match &import.path {
            Some(path) => {
                let pkg_dir = pkg_dir.join(path);
                let mut package = read_imported_package_config(&import.metadata, &pkg_dir, parser)?;

                add_package_tree(
                    res,
                    &pkg_dir,
                    &mut package.imports,
                    package.dev.as_ref(),
                    debug,
                    parser,
                )?;

                res.push(package);
            }
            None => {
                let not_found_msg = format!("Package {} not found on Hub.", import.metadata);

                if let Some(dev_config) = dev_imports {
                    if dev_override_path_for_package(&import.metadata, &dev_config.imports)
                        .is_some()
                    {
                        return Err(anyhow!("{} {}", not_found_msg, unused_dev_override_msg()));
                    }
                }

                if debug {
                    return Err(anyhow!(
                        "{} {}",
                        not_found_msg,
                        override_missing_in_dev_mode_msg()
                    ));
                } else {
                    return Err(anyhow!(
                        "{} {}",
                        not_found_msg,
                        dev_override_instruction_msg()
                    ));
                }
            }
        }
    }

    Ok(())
}

fn set_dev_overrides(imports: &mut [PackageImport], dev_imports: &[PackageImport]) {
    for import in imports.iter_mut() {
        if let Some(path) = dev_override_path_for_package(&import.metadata, dev_imports) {
            import.path = Some(path.to_string());
        }
    }
}

fn dev_override_path_for_package<'a>(
    package_metadata: &Header,
    dev_imports: &'a [PackageImport],
) -> Option<&'a str> {
    for dev_import in dev_imports.iter() {
        if package_metadata == &dev_import.metadata {
            return dev_import.path.as_deref();
        }
    }

    None
}

pub fn read_imported_package_config(
    header: &Header,
    package_dir: impl AsRef<Path>,
    parser: &mut HostParser,
) -> Result<PackageDefinition> {
    let pkg_dir_path = package_dir.as_ref();
    let package_path = pkg_dir_path.join(DEFAULT_PACKAGE_FILE);

    info!(file=?package_path.display(), "reading package file");

    let config_string = std::fs::read_to_string(&package_path).map_err(|_e| {
        anyhow!(
            "failed to load package config: expected to find package: {} at {}",
            header,
            package_path.display()
        )
    })?;

    parser.parse_package(&config_string)
}

fn override_missing_in_dev_mode_msg() -> String {
    [
        "SDF is running in Dev mode.",
        " Did you mean to import the package from the local filesystem?",
        " If so, please specify a path in the development config",
    ]
    .concat()
}

fn dev_override_instruction_msg() -> String {
    [
        "If you would like to import the package from the local filesystem,",
        " please specify a path in the development config and pass the `--dev` flag",
    ]
    .concat()
}

fn unused_dev_override_msg() -> String {
    [
        "An override path for this package was specified in the development config.",
        " please pass `--dev` to use the local override of the package.",
    ]
    .concat()
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{fetch_package_configs, set_dev_overrides};

    use sdf_metadata::wit::package_interface::{DevConfig, Header, PackageImport};
    use sdf_parser_host::host::HostParser;

    fn imports() -> Vec<PackageImport> {
        vec![PackageImport {
            metadata: Header {
                namespace: "example".to_string(),
                name: "bank-update".to_string(),
                version: "0.1.0".to_string(),
            },
            path: None,
            types: vec![],
            states: vec![],
            functions: vec![],
        }]
    }

    #[test]
    fn test_set_dev_overrides_sets_path() {
        let mut imports = imports();

        let dev_imports = vec![PackageImport {
            metadata: Header {
                namespace: "example".to_string(),
                name: "bank-update".to_string(),
                version: "0.1.0".to_string(),
            },
            path: Some("test/bank-update".to_string()),
            types: vec![],
            states: vec![],
            functions: vec![],
        }];

        set_dev_overrides(&mut imports, &dev_imports);

        assert_eq!(imports[0].path, Some("test/bank-update".to_string()));
    }

    #[fluvio_future::test]
    async fn test_fetch_package_configs_fetches_dev_configs() {
        let debug = true;
        let mut imports = imports();

        let dev_config = DevConfig {
            converter: None,
            imports: vec![PackageImport {
                metadata: Header {
                    namespace: "example".to_string(),
                    name: "bank-update".to_string(),
                    version: "0.1.0".to_string(),
                },
                path: Some("test/bank-update".to_string()),
                types: vec![],
                states: vec![],
                functions: vec![],
            }],
            topics: vec![],
        };

        let pkg_dir = PathBuf::from(".");

        let parser = &mut HostParser::new();
        let res = fetch_package_configs(&pkg_dir, &mut imports, Some(&dev_config), debug, parser)
            .expect("Failed to fetch package configs");

        assert_eq!(res.len(), 2);
        assert_eq!(res[1].meta.name, "bank-update");
        assert_eq!(res[0].meta.name, "bank");
    }

    #[fluvio_future::test]
    async fn test_fetch_package_configs_details_missing_local_config_file() {
        let debug = true;
        let mut imports = imports();

        let dev_config = DevConfig {
            converter: None,
            imports: vec![PackageImport {
                metadata: Header {
                    namespace: "example".to_string(),
                    name: "bank-update".to_string(),
                    version: "0.1.0".to_string(),
                },
                path: Some("foobar-path".to_string()),
                types: vec![],
                states: vec![],
                functions: vec![],
            }],
            topics: vec![],
        };

        let pkg_dir = PathBuf::from(".");
        let parser = &mut HostParser::new();

        let res = fetch_package_configs(&pkg_dir, &mut imports, Some(&dev_config), debug, parser)
            .expect_err("should fail to read config file");

        assert_eq!(
            res.to_string(),
            "failed to load package config: expected to find package: example/bank-update@0.1.0 at ./foobar-path/sdf-package.yaml"
        )
    }

    // dev mode, no override set
    #[fluvio_future::test]
    async fn test_fetch_package_configs_suggests_defining_local_override_in_debug_mode() {
        let debug = true;
        let mut imports = imports();

        let dev_config = DevConfig {
            converter: None,
            imports: vec![],
            topics: vec![],
        };

        let pkg_dir = PathBuf::from(".");
        let parser = &mut HostParser::new();
        let res = fetch_package_configs(&pkg_dir, &mut imports, Some(&dev_config), debug, parser)
            .expect_err("should fail to find package on hub");

        assert_eq!(
            res.to_string(),
            [
                "Package example/bank-update@0.1.0 not found on Hub. SDF is running in Dev mode.",
                " Did you mean to import the package from the local filesystem?",
                " If so, please specify a path in the development config"
            ]
            .concat()
        )
    }

    // prod mode, no override set
    #[fluvio_future::test]
    async fn test_fetch_package_configs_explains_dev_override_when_hub_package_not_found() {
        let debug = false;
        let mut imports = imports();

        let dev_config = DevConfig {
            converter: None,
            imports: vec![],
            topics: vec![],
        };

        let pkg_dir = PathBuf::from(".");
        let parser = &mut HostParser::new();
        let res = fetch_package_configs(&pkg_dir, &mut imports, Some(&dev_config), debug, parser)
            .expect_err("should fail to find package on hub");

        assert_eq!(
            res.to_string(),
            [
                "Package example/bank-update@0.1.0 not found on Hub.",
                " If you would like to import the package from the local filesystem,",
                " please specify a path in the development config and pass the `--dev` flag"
            ]
            .concat()
        )
    }

    // prod mode, override found
    #[fluvio_future::test]
    async fn test_fetch_package_configs_suggests_using_existing_local_override() {
        let debug = false;
        let mut imports = imports();

        let dev_config = DevConfig {
            converter: None,
            imports: vec![PackageImport {
                metadata: Header {
                    namespace: "example".to_string(),
                    name: "bank-update".to_string(),
                    version: "0.1.0".to_string(),
                },
                path: Some("foobar-path".to_string()),
                types: vec![],
                states: vec![],
                functions: vec![],
            }],
            topics: vec![],
        };

        let pkg_dir = PathBuf::from(".");
        let parser = &mut HostParser::new();
        let res = fetch_package_configs(&pkg_dir, &mut imports, Some(&dev_config), debug, parser)
            .expect_err("should fail to find package on hub");

        assert_eq!(
            res.to_string(),
            [
                "Package example/bank-update@0.1.0 not found on Hub.",
                " An override path for this package was specified in the development config.",
                " please pass `--dev` to use the local override of the package."
            ]
            .concat()
        )
    }
}
