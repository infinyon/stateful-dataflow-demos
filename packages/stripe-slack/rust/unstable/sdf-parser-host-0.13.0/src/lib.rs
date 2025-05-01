use anyhow::Result;

pub mod host;

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "parser-guest",
        async: false,
        with: {
            "sdf:metadata/dataflow": sdf_metadata::wit::dataflow,
            "sdf:metadata/package-interface": sdf_metadata::wit::package_interface
        },
    });
}

pub fn parse_package(package: &str) -> Result<sdf_metadata::wit::dataflow::PackageDefinition> {
    let mut parser = host::HostParser::new();
    parser.parse_package(package)
}

pub fn parse_dataflow(dataflow: &str) -> Result<sdf_metadata::wit::dataflow::DataflowDefinition> {
    let mut parser = host::HostParser::new();
    parser.parse_dataflow(dataflow)
}
