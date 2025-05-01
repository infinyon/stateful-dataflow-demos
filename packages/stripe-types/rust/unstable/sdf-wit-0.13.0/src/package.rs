use std::path::PathBuf;

use anyhow::{Result, Context};
use bon::Builder;
use wit_encoder::{Package, PackageName, World};

use sdf_parser_host::host::HostParser;

#[derive(Builder)]
pub struct WitGenerator {
    #[builder(default = PathBuf::from("../../sdf-package.yaml"))]
    pkg_path: PathBuf,
    #[builder(default = PathBuf::from("."))]
    target_dir: PathBuf,
    #[builder(default = true)]
    dev_mode: bool,
}

impl WitGenerator {
    /// check if pkg path last modification time is newer than the output dir
    fn should_regenerate(&self) -> bool {
        if self.pkg_path.exists() {
            let Ok(pkg_metadata) = std::fs::metadata(&self.pkg_path) else {
                return true;
            };
            if pkg_metadata.modified().is_ok() {
                let output_dir = self.target_dir.join(".wit");
                if output_dir.exists() {
                    let Ok(output_metadata) = std::fs::metadata(&output_dir) else {
                        return true;
                    };

                    let Ok(pkg_modified) = pkg_metadata.modified() else {
                        return true;
                    };

                    let Ok(output_modified) = output_metadata.modified() else {
                        return true;
                    };
                    if pkg_modified <= output_modified {
                        return false;
                    }
                } else {
                    return true;
                }
            }
        }
        true
    }

    pub fn generate(&self) -> Result<PathBuf> {
        if !self.should_regenerate() {
            return Ok(self.target_dir.join(".wit"));
        }

        let output_dir = self.target_dir.join(".wit");

        let deps_dir = output_dir.join("deps");

        super::generate_common_wit_deps(&deps_dir)?;

        let mut parser = HostParser::new();

        let file_content = std::fs::read_to_string(&self.pkg_path).context(format!(
            "failed to read pkg path: {}",
            self.pkg_path.display()
        ))?;

        let path = self.pkg_path.parent().expect("failed to get pkg path");

        let mut sdf_package = parser.parse_package(&file_content)?;

        let package_deps = sdf_imports::fetch_package_configs(
            path,
            &mut sdf_package.imports,
            sdf_package.dev.as_ref(),
            self.dev_mode,
            &mut parser,
        )?;

        let api_version = &sdf_package.api_version()?;

        sdf_package.resolve_imports(package_deps, self.dev_mode)?;
        let types = sdf_package.types_map();
        let types_iface = types.wit_interface(api_version, vec![]);

        let meta = &sdf_package.meta;

        let meta_name = &meta.name;
        let meta_namespace = &meta.namespace;

        let service_names_exports = sdf_package
            .functions
            .iter()
            .map(|(f, _)| format!("{}-service", f.uses))
            .collect::<Vec<_>>();

        let name = PackageName::new(meta_namespace.to_string(), meta_name.to_string(), None);

        let mut package = Package::new(name);

        let mut default_world = World::new("default-world");
        default_world.named_interface_export("types");
        for service_name in service_names_exports {
            default_world.named_interface_export(service_name);
        }

        package.world(default_world);
        package.interface(types_iface);

        // we need to generate the interface for each function
        for (function, op_type) in &sdf_package.functions {
            let iface = function.wit_interface(op_type);

            package.interface(iface);

            let iface = function.deserialize_input_wit_interface();
            let world_name = format!("{}-world", iface.name());
            let mut world = World::new(world_name);
            world.named_interface_export(iface.name().to_owned());

            package.interface(iface);
            package.world(world);

            if let Some(serialize_interface) = function.serialize_output_wit_interface() {
                let world_name = format!("{}-world", serialize_interface.name());
                let mut world = World::new(world_name);
                world.named_interface_export(serialize_interface.name().to_owned());
                package.interface(serialize_interface);
                package.world(world);
            }
        }

        let wit_default_file_path = output_dir.join("api.wit");
        std::fs::write(&wit_default_file_path, package.to_string())?;

        // add .gitignore

        let gitignore_path = output_dir.join(".gitignore");
        std::fs::write(&gitignore_path, "*")?;

        Ok(output_dir)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_wit_generator() {
        let temp_dir = tempfile::tempdir().unwrap();

        let sdf_pkg = r#"
apiVersion: 0.5.0
meta:
  name: first-word
  version: 0.1.0
  namespace: pkg-namespace

functions:
  first-word-len:
    operator: filter-map
    inputs:
      - name: input
        type: string
    output:
      type: u32
      optional: true
        "#;
        let pkg_path = temp_dir.path().join("sdf-package.yaml");
        std::fs::write(&pkg_path, sdf_pkg).unwrap();
        let generator = super::WitGenerator::builder()
            .pkg_path(pkg_path.clone())
            .target_dir(temp_dir.path().to_path_buf())
            .build();

        assert!(generator.should_regenerate());

        generator
            .generate()
            .expect("failed to generate wit package");
        assert!(!generator.should_regenerate());
    }
}
