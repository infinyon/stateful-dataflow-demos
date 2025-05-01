use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};

use crate::wit::{
    dataflow::PackageImport,
    package_interface::{PackageDefinition, StateTyped},
};

use super::sdf_types_map::SdfTypesMap;

pub fn merge_types_and_states(
    all_types: &mut SdfTypesMap,
    all_states: &mut BTreeMap<String, StateTyped>,
    imports: &[PackageImport],
    package_configs: &[PackageDefinition],
) -> Result<()> {
    for import in imports.iter() {
        let pkg_config = package_configs
            .iter()
            .find(|pkg| import.metadata == pkg.meta)
            .context("package not found")?;

        let pkg_types = pkg_config.types_map();

        for ty in &import.types {
            let imported_ty = pkg_types.get_type_tree(ty);

            for (name, imported_ty) in imported_ty.iter() {
                if let Some(prev) = all_types.insert_imported(name.clone(), imported_ty.clone()) {
                    if &prev.0 != imported_ty {
                        return Err(anyhow!(
                            "imported type {} from {} conflicts with existing type",
                            name,
                            import.metadata.name
                        ));
                    }
                }
            }
        }

        for state in &import.states {
            let state = pkg_config
                .states
                .iter()
                .find(|s| s.name == *state)
                .ok_or_else(|| {
                    anyhow!(
                        "state {} not found in imported package {}",
                        state,
                        import.metadata.name
                    )
                })?;
            let imported_ty = pkg_types.get_type_tree(&state.name);
            for (name, imported_ty) in imported_ty.iter() {
                if let Some(prev) = all_types.insert_imported(name.clone(), imported_ty.clone()) {
                    if &prev.0 != imported_ty {
                        return Err(anyhow!(
                            "imported type {} from {} conflicts with existing type",
                            name,
                            import.metadata.name
                        ));
                    }
                }
            }

            all_states.insert(state.name.clone(), state.clone());
        }
    }

    resolve_states(all_states, all_types)?;

    Ok(())
}

fn resolve_states(states: &mut BTreeMap<String, StateTyped>, types: &SdfTypesMap) -> Result<()> {
    for state in states.values_mut() {
        state.resolve(types)?;
    }

    Ok(())
}
