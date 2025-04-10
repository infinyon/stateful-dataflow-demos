use std::{fs, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=../../sdf-package.yaml");
    println!("cargo:rerun-if-changed=../.sdf/.wit");

    let pkg_dir = fs::canonicalize("../../").expect("pkg path");
    println!("cargo:warning=using pkg path {}", pkg_dir.display());
    // Fetch current git hash to print version output
    let sdf_output = Command::new("sdf")
        .current_dir(pkg_dir)
        .arg("build")
        .arg("--wit")
        .output();

    if sdf_output.is_err() {
        println!(
            "cargo:warning=failed to build sdf package: {:#?}",
            sdf_output
        );
    }
}
