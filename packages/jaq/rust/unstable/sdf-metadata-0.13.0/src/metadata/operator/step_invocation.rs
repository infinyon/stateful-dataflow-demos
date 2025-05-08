use std::collections::BTreeSet;

use anyhow::Result;
use sdf_common::render::wit_name_case;
use wit_encoder::{Interface, Params, Result_, StandaloneFunc, Type, Use};

use sdf_common::{constants::ROW_VALUE_WIT_TYPE, render::map_wit_keyword};
use sdf_common::constants::{DF_VALUE_WIT_TYPE, I32_LIST_VALUE_WIT_TYPE, I32_VALUE_WIT_TYPE};

use crate::wit::io::TypeRef;
use crate::{
    metadata::metadata::sdf_type::hashable_primitives_list,
    util::{
        sdf_types_map::{is_imported_type, SdfTypesMap},
        validation_failure::ValidationFailure,
    },
    wit::{
        dataflow::PackageImport,
        metadata::{NamedParameter, OutputType, Parameter, ParameterKind, SdfType},
        operator::{CodeInfo, ImportedFunctionMetadata, OperatorType, StepInvocation, StepState},
        states::StateTyped,
    },
};

impl StepInvocation {
    pub fn set_inputs(&mut self, inputs: Vec<NamedParameter>) {
        self.inputs = inputs;
    }

    pub fn set_output(&mut self, output: Option<Parameter>) {
        self.output = output
    }

    pub fn set_imported_function_metadata(&mut self, meta: Option<ImportedFunctionMetadata>) {
        self.imported_function_metadata = meta
    }

    pub fn set_states(&mut self, states: Vec<StepState>) {
        self.states = states;
    }

    pub fn is_imported(&self, imports: &[PackageImport]) -> bool {
        let name = &self.uses;

        imports.iter().any(|import| {
            import.functions.iter().any(|function| {
                if let Some(alias) = &function.alias {
                    *alias == *name
                } else {
                    function.name == *name
                }
            })
        })
    }

    pub fn resolve_states(&mut self, states: &[StateTyped]) -> Result<()> {
        for state in &mut self.states {
            state.resolve(states)?;
        }

        Ok(())
    }

    pub fn requires_key_param(&self) -> bool {
        self.inputs
            .first()
            .map(|input| matches!(input.kind, ParameterKind::Key))
            .unwrap_or_default()
    }

    pub fn has_key_in_output(&self) -> bool {
        self.output
            .as_ref()
            .map(|output| matches!(output.type_, OutputType::KeyValue(_)))
            .unwrap_or_default()
    }

    #[cfg(feature = "parser")]
    fn validate_code(&self) -> Result<(), ValidationFailure> {
        use crate::util::sdf_function_parser::SDFFunctionParser;
        let mut errors = ValidationFailure::new();

        if let Some(ref code) = self.code_info.code {
            if code.is_empty() {
                errors.push_str("Code block is empty");
            }

            match SDFFunctionParser::parse(&self.code_info.lang, code) {
                Ok((uses, inputs, output)) => {
                    // must match with self.attributes
                    if self.uses != uses {
                        errors.push_str(&format!(
                            "function name on parsed code does not match. Got {uses}, expected: {}",
                            self.uses
                        ))
                    }
                    if self.output != output {
                        errors.push_str(&format!(
                            "function output on parsed code does not match. Got {:?}, expected: {:?}", output, self.output,
                        ))
                    }

                    if self.inputs != inputs {
                        errors.push_str(&format!(
                            "function input on parsed code does not match. Got {:?}, expected: {:?}", inputs, self.inputs,
                        ))
                    }
                }
                Err(err) => {
                    errors.push_str(&err.to_string());
                }
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_map(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "map") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_present("map") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_filter_map(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "filter-map") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_present("filter-map") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_is_optional("filter-map", types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_filter(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "filter") {
            errors.concat(&err);
        }

        if !self
            .output
            .as_ref()
            .map(|p| p.is_bool())
            .unwrap_or_default()
        {
            errors.push_str(&format!(
                "filter type function `{}` requires an output type of `bool`, but found {}",
                self.uses,
                self.output
                    .as_ref()
                    .map(|p| format!("`{}`", p.type_.value_type_name()))
                    .unwrap_or("no type".to_string())
            ));
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_flat_map(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "flat-map") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_present("flat-map") {
            errors.concat(&err);
        }
        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_update_state(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "update-state") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Some(output) = &self.output {
            errors.push_str(&format!(
                "update-state type function `{}` should have no output, but found `{}`",
                self.uses,
                output.type_.value_type_name()
            ));
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_window_aggregate(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if !self.inputs.is_empty() {
            errors.push_str(&format!(
                "window-aggregate type function `{}` should have no input type, but found {}",
                self.uses,
                self.inputs
                    .iter()
                    .map(|p| format!("[{}: {}]", p.name, p.type_.name))
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }

        if let Err(err) = self.validate_output_present("window-aggregate") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_assign_key(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(1, "assign-key") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_present("assign-key") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        if let Some(output) = &self.output {
            let output_type_name = output.type_.value_type_name();

            if let Some((output_type, _)) = types.get(output_type_name) {
                if !output_type.is_hashable(types) {
                    errors.push_str(&format!(
                        "output type for assign-key type function `{}` must be hashable, or a reference to a hashable type. found `{}`.\n hashable types: [{}]",
                        self.uses, output_type_name, hashable_primitives_list()
                    ));
                }
            }
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    pub fn validate_assign_timestamp(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        if let Err(err) = self.validate_n_value_input(2, "assign-timestamp") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_present("assign-timestamp") {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_inputs_in_scope(types) {
            errors.concat(&err);
        }

        if let Err(err) = self.validate_output_in_scope(types) {
            errors.concat(&err);
        }

        if let Some(second_input) = self.inputs.last() {
            if types.is_s64(&second_input.type_.name) {
            } else {
                errors.push_str(
                    format!(
                        "second input type for assign-timestamp type function `{}` must be a signed 64-bit int or an alias for one, found: `{}`",
                        self.uses, second_input.type_.name
                    )
                    .as_str(),
                );
            }
        }

        if let Some(output) = &self.output {
            let output_type_name = &output.type_.value_type_name();

            if !types.is_s64(output_type_name) {
                errors.push_str(&format!(
                        "output type for assign-timestamp type function `{}` must be a signed 64-bit int or an alias for one, found: `{}`",
                        self.uses, output_type_name
                    ));
            }
        }

        #[cfg(feature = "parser")]
        if let Err(err) = self.validate_code() {
            errors.concat(&err);
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_n_value_input(
        &self,
        n: usize,
        function_type: &str,
    ) -> Result<(), ValidationFailure> {
        let Some(first_input) = self.inputs.first() else {
            return Err(ValidationFailure::from(
                format!(
                    "{} type function `{}` should have exactly {} input type, found 0",
                    function_type, self.uses, n
                )
                .as_str(),
            ));
        };

        let expected_types = n + (first_input.kind == ParameterKind::Key) as usize;

        if self.inputs.len() != expected_types {
            return Err(ValidationFailure::from(
                format!(
                    "{} type function `{}` should have exactly {} input type, found {}",
                    function_type,
                    self.uses,
                    expected_types,
                    self.inputs.len()
                )
                .as_str(),
            ));
        }

        Ok(())
    }

    fn validate_output_present(&self, function_type: &str) -> Result<(), ValidationFailure> {
        if self.output.is_none() {
            return Err(ValidationFailure::from(
                format!(
                    "{} type function `{}` requires an output type",
                    function_type, self.uses
                )
                .as_str(),
            ));
        }

        Ok(())
    }

    fn validate_output_is_optional(
        &self,
        function_type: &str,
        types_map: &SdfTypesMap,
    ) -> Result<(), ValidationFailure> {
        if let Some(output) = &self.output {
            if output.optional {
                return Ok(());
            }

            if let OutputType::Ref(ref ty) = output.type_ {
                if let Some(resolved_type) = types_map.inner_type_name(&ty.name) {
                    if let Some((ty, _)) = types_map.get(&resolved_type) {
                        if matches!(ty, SdfType::Option(_)) {
                            return Ok(());
                        }
                    }
                }
            }
        }

        Err(ValidationFailure::from(
            format!(
                "{} type function `{}` requires an optional output type",
                function_type, self.uses
            )
            .as_str(),
        ))
    }

    fn validate_inputs_in_scope(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        let mut errors = ValidationFailure::new();

        for input in &self.inputs {
            if !types.contains_key(&input.type_.name) {
                errors.push_str(
                    format!(
                        "function `{}` has invalid input type, {}",
                        self.uses,
                        ref_type_error(&input.type_.name)
                    )
                    .as_str(),
                );
            }
        }

        if errors.any() {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_output_in_scope(&self, types: &SdfTypesMap) -> Result<(), ValidationFailure> {
        if let Some(output) = &self.output {
            if !types.contains_key(output.type_.value_type_name()) {
                return Err(ValidationFailure::from(
                    format!(
                        "function `{}` has invalid output type, {}",
                        self.uses,
                        ref_type_error(output.type_.value_type_name())
                    )
                    .as_str(),
                ));
            }
        }

        Ok(())
    }

    #[cfg(feature = "parser")]
    pub fn update_signature_from_code(&mut self) -> Result<()> {
        use crate::util::sdf_function_parser::SDFFunctionParser;
        let mut errors = ValidationFailure::new();

        if let Some(ref code) = self.code_info.code {
            if code.is_empty() {
                errors.push_str("Code block is empty");
            }

            match SDFFunctionParser::parse(&self.code_info.lang, code) {
                Ok((uses, inputs, output)) => {
                    self.output = output;
                    self.inputs = inputs;
                    self.uses = uses;
                }
                Err(err) => {
                    errors.push_str(&err.to_string());
                }
            }
        }

        if errors.any() {
            Err(anyhow::anyhow!("{}", errors))
        } else {
            Ok(())
        }
    }

    fn wit_function(&self, op_type: &OperatorType) -> StandaloneFunc {
        let mut operator_fn = StandaloneFunc::new(self.uses.to_owned(), false);

        let mut params = Params::empty();

        for input in &self.inputs {
            let ty = input.type_.wit_type();
            let ty = if input.optional || matches!(input.kind, ParameterKind::Key) {
                Type::Option(Box::new(ty))
            } else {
                ty
            };

            params.item(input.name.to_string(), ty);
        }
        operator_fn.set_params(params);

        let results = match self.output.as_ref() {
            None => Some(Type::Result(Box::new(Result_::err(Type::String)))),
            Some(output) => {
                let ty = output.type_.wit_type();
                let ty = if output.optional {
                    Type::Option(Box::new(ty))
                } else if matches!(op_type, OperatorType::FlatMap) {
                    Type::List(Box::new(ty))
                } else {
                    ty
                };

                Some(Type::Result(Box::new(Result_::both(ty, Type::String))))
            }
        };

        operator_fn.set_result(results);

        operator_fn
    }

    pub fn wit_interface(&self, op_type: &OperatorType) -> Interface {
        let mut interface = Interface::new(format!("{}-service", self.uses));

        if let Some(imported_types) = self.imported_types() {
            interface.use_(imported_types);
        }

        for import in self.state_imports(op_type) {
            interface.use_(import);
        }

        let wit_fn = self.wit_function(op_type);

        interface.function(wit_fn);

        interface
    }

    pub fn state_imports(&self, op_type: &OperatorType) -> Vec<Use> {
        self.states
            .iter()
            .filter_map(|s| match s {
                StepState::Resolved(state) => match &state.type_.value {
                    crate::wit::metadata::SdfKeyedStateValue::ArrowRow(_) => {
                        if let OperatorType::WindowAggregate = op_type {
                            let mut use_value = Use::new("sdf:df/lazy");
                            use_value.item(DF_VALUE_WIT_TYPE, None);

                            Some(use_value)
                        } else {
                            let mut use_value = Use::new("sdf:row-state/row");
                            use_value.item(ROW_VALUE_WIT_TYPE, None);

                            Some(use_value)
                        }
                    }
                    crate::wit::metadata::SdfKeyedStateValue::U32 => {
                        let mut use_value = Use::new("sdf:value-state/values");

                        if let OperatorType::WindowAggregate = op_type {
                            use_value.item(I32_LIST_VALUE_WIT_TYPE, None);
                            Some(use_value)
                        } else {
                            use_value.item(I32_VALUE_WIT_TYPE, None);
                            Some(use_value)
                        }
                    }
                    crate::wit::metadata::SdfKeyedStateValue::Unresolved(_) => None,
                },
                StepState::Unresolved(_) => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn imported_types(&self) -> Option<Use> {
        let types_to_import = self
            .inputs
            .iter()
            .map(|iter| iter.type_.name.clone())
            .chain(
                self.output
                    .as_ref()
                    .map(|output| output.type_.value_type_name().to_owned()),
            )
            .chain(
                self.output
                    .as_ref()
                    .and_then(|output| output.type_.key_type_name().map(|s| s.to_owned())),
            )
            .chain(self.states.iter().filter_map(|s| match s {
                StepState::Resolved(state) => Some(state.name.to_owned()),
                StepState::Unresolved(_) => None,
            }))
            .collect::<BTreeSet<_>>();

        let items = types_to_import
            .into_iter()
            .filter(|t| is_imported_type(t))
            .map(|t| map_wit_keyword(&t))
            .collect::<Vec<_>>();

        if items.is_empty() {
            return None;
        }

        let mut uses = Use::new("types");
        for item in items {
            uses.item(wit_name_case(&item), None);
        }

        Some(uses)
    }

    pub fn deserialize_input_wit_interface(&self) -> Interface {
        let mut iface = Interface::new(format!("deserialize-{}", self.uses));

        let mut deserialize_key_fn = StandaloneFunc::new("deserialize-key", false);
        let mut params = Params::empty();
        params.item("key", Type::option(Type::String));
        deserialize_key_fn.set_params(params);

        let mut imported_types = BTreeSet::new();

        if let Some(key_type) = self.input_key_type() {
            deserialize_key_fn.set_result(Some(Type::result_both(
                Type::option(key_type.wit_type()),
                Type::String,
            )));

            if is_imported_type(&key_type.name) {
                imported_types.insert(map_wit_keyword(&key_type.name));
            }
        } else {
            deserialize_key_fn.set_result(Some(Type::result_both(
                Type::option(Type::list(Type::U8)),
                Type::String,
            )));
        }

        iface.function(deserialize_key_fn);

        let mut deserialize_value_fn = StandaloneFunc::new("deserialize-input", false);
        let mut params = Params::empty();
        params.item("value", Type::String);
        deserialize_value_fn.set_params(params);

        if let Some(value_type) = self.input_value_type() {
            deserialize_value_fn
                .set_result(Some(Type::result_both(value_type.wit_type(), Type::String)));
            if is_imported_type(&value_type.name) {
                imported_types.insert(map_wit_keyword(&value_type.name));
            }
        } else {
            deserialize_value_fn
                .set_result(Some(Type::result_both(Type::list(Type::U8), Type::String)));
        }

        if !imported_types.is_empty() {
            let mut uses = Use::new("types");
            for ty in imported_types {
                uses.item(wit_name_case(&ty), None);
            }

            iface.use_(uses);
        }
        iface.function(deserialize_value_fn);

        iface
    }

    pub fn serialize_output_wit_interface(&self) -> Option<Interface> {
        let output_value = self.output_value_type()?;

        let mut iface = Interface::new(format!("serialize-{}", self.uses));
        let mut imported_types = BTreeSet::new();

        let mut serialize_key_fn = StandaloneFunc::new("serialize-key", false);
        let mut params = Params::empty();
        if let Some(output_key) = self.output_key_type() {
            params.item("input", Type::option(output_key.wit_type()));
            if is_imported_type(&output_key.name) {
                imported_types.insert(map_wit_keyword(&output_key.name));
            }
        } else {
            params.item("input", Type::option(Type::list(Type::U8)));
        }
        serialize_key_fn.set_params(params);

        serialize_key_fn.set_result(Some(Type::result_both(
            Type::option(Type::list(Type::U8)),
            Type::String,
        )));

        iface.function(serialize_key_fn);
        let mut serialize_value_fn = StandaloneFunc::new("serialize-output", false);
        let mut params = Params::empty();

        if is_imported_type(&output_value.name) {
            imported_types.insert(map_wit_keyword(&output_value.name));
        }
        params.item("input", output_value.wit_type());
        serialize_value_fn.set_params(params);
        serialize_value_fn.set_result(Some(Type::result_both(Type::list(Type::U8), Type::String)));
        iface.function(serialize_value_fn);
        if !imported_types.is_empty() {
            let mut uses = Use::new("types");
            for ty in imported_types {
                uses.item(wit_name_case(&ty), None);
            }

            iface.use_(uses);
        }
        Some(iface)
    }

    fn input_key_type(&self) -> Option<TypeRef> {
        self.inputs
            .iter()
            .find(|input| matches!(input.kind, ParameterKind::Key))
            .map(|input| input.type_.clone())
    }

    fn input_value_type(&self) -> Option<TypeRef> {
        self.inputs
            .iter()
            .find(|input| matches!(input.kind, ParameterKind::Value))
            .map(|input| input.type_.clone())
    }

    fn output_key_type(&self) -> Option<TypeRef> {
        self.output
            .as_ref()
            .and_then(|output| output.type_.key_type())
            .cloned()
    }

    fn output_value_type(&self) -> Option<TypeRef> {
        self.output
            .as_ref()
            .map(|output| output.type_.value_type())
            .cloned()
    }
}

fn ref_type_error(name: &str) -> String {
    format!(
        "Referenced type `{}` not found in config or imported types",
        name
    )
}

#[allow(clippy::derivable_impls)]
impl Default for StepInvocation {
    fn default() -> Self {
        StepInvocation {
            uses: String::new(),
            inputs: Vec::new(),
            output: None,
            states: Vec::new(),
            imported_function_metadata: None,
            code_info: CodeInfo::default(),
            system: false,
            params: None,
        }
    }
}

#[cfg(test)]
mod test {
    use sdf_common::display::WitInterfaceDisplay;

    use crate::{
        metadata::operator::step_invocation::ref_type_error,
        util::{sdf_types_map::SdfTypesMap, validation_error::ValidationError},
        wit::{
            io::TypeRef,
            metadata::{
                NamedParameter, Parameter, ParameterKind, SdfArrowRow, SdfKeyValue, SdfType,
            },
            operator::{OperatorType, StepInvocation},
        },
    };
    #[cfg(feature = "parser")]
    use crate::wit::operator::{CodeInfo, CodeLang};

    fn types() -> SdfTypesMap {
        SdfTypesMap::default()
    }

    #[test]
    fn test_validate_filter_requires_one_input() {
        let function = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "filter type function `my-filter` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "filter type function `my-filter` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_filter_requires_bool_output() {
        let function = StepInvocation {
            uses: "my-filter".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "filter type function `my-filter` requires an output type of `bool`, but found no type"
        )));

        let function = StepInvocation {
            uses: "my-filter".to_string(),
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "filter type function `my-filter` requires an output type of `bool`, but found `string`"
        )));
    }

    #[test]
    fn test_validate_filter_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-filter` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_filter_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-filter` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_filter_accepts_valid_functions() {
        let function = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "bool".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function.validate_filter(&types()).expect("should validate");
    }

    #[test]
    fn test_validate_map_requires_one_input() {
        let function = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_map(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "map type function `my-map` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_map(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "map type function `my-map` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_map_requires_one_output() {
        let function = StepInvocation {
            uses: "my-map".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_map(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "map type function `my-map` requires an output type"
        )));
    }

    #[test]
    fn test_validate_map_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_map(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-map` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_map_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_map(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-map` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_map_accepts_valid_functions() {
        let function = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function.validate_map(&types()).expect("should validate");
    }

    #[test]
    fn test_validate_flat_map_requires_one_input() {
        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_flat_map(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "flat-map type function `my-flat-map` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_flat_map(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "flat-map type function `my-flat-map` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_flat_map_requires_one_output() {
        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_flat_map(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "flat-map type function `my-flat-map` requires an output type"
        )));
    }

    #[test]
    fn test_validate_flat_map_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_flat_map(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-flat-map` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_flat_map_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_flat_map(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-flat-map` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_flat_map_accepts_valid_functions() {
        let function = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function
            .validate_flat_map(&types())
            .expect("should validate");
    }

    #[test]
    fn test_validate_filter_map_requires_one_input() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "filter-map type function `my-filter-map` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "filter-map type function `my-filter-map` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_filter_map_requires_one_output() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "filter-map type function `my-filter-map` requires an output type"
        )));
    }

    #[test]
    fn test_validate_filter_map_requires_optional_output() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "filter-map type function `my-filter-map` requires an optional output type"
        )));
    }

    #[test]
    fn test_validate_filter_map_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                optional: true,
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-filter-map` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_filter_map_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_filter_map(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-filter-map` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_filter_map_accepts_valid_functions() {
        let function = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                optional: true,
            }),
            ..Default::default()
        };

        function
            .validate_filter_map(&types())
            .expect("should validate");
    }

    #[test]
    fn test_validate_update_state_requires_one_input() {
        let function = StepInvocation {
            uses: "my-update-state".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_update_state(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "update-state type function `my-update-state` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-update-state".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_update_state(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "update-state type function `my-update-state` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_update_state_requires_no_output() {
        let function = StepInvocation {
            uses: "my-update-state".to_string(),
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_update_state(&types())
            .expect_err("should error for unexpected output type");

        assert!(res.errors.contains(&ValidationError::new(
            "update-state type function `my-update-state` should have no output, but found `string`"
        )));
    }

    #[test]
    fn test_validate_update_state_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-update-state".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_update_state(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-update-state` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_update_state_accepts_valid_function() {
        let function = StepInvocation {
            uses: "my-update-state".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            ..Default::default()
        };

        function
            .validate_update_state(&types())
            .expect("should validate");
    }

    #[test]
    fn test_validate_window_aggregate_requires_no_input() {
        let function = StepInvocation {
            uses: "my-window-aggregate".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            ..Default::default()
        };

        let res = function
            .validate_window_aggregate(&types())
            .expect_err("should error for unexpected input type");

        assert!(res.errors.contains(&ValidationError::new(
            "window-aggregate type function `my-window-aggregate` should have no input type, but found [input: string]"
        )));
    }

    #[test]
    fn test_validate_window_aggregate_requires_one_output() {
        let function = StepInvocation {
            uses: "my-window-aggregate".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_window_aggregate(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "window-aggregate type function `my-window-aggregate` requires an output type"
        )));
    }

    #[test]
    fn test_validate_window_aggregate_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-window-aggregate".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_window_aggregate(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-window-aggregate` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_window_aggregate_accepts_valid_functions() {
        let function = StepInvocation {
            uses: "my-window-aggregate".to_string(),
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function
            .validate_window_aggregate(&types())
            .expect("should validate");
    }

    #[test]
    fn test_validate_assign_key_requires_one_input() {
        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![],
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types())
            .expect_err("should error for missing input type");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-key type function `my-assign-key` should have exactly 1 input type, found 0"
        )));

        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-key type function `my-assign-key` should have exactly 1 input type, found 2"
        )));
    }

    #[test]
    fn test_validate_assign_key_requires_one_output() {
        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-key type function `my-assign-key` requires an output type"
        )));
    }

    #[test]
    fn test_validate_assign_key_requires_input_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "foobar".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-assign-key` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_assign_key_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],

            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-assign-key` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_assign_key_requires_output_type_is_hashable() {
        let mut types = types();
        types.insert_local(
            "my-state-value".to_string(),
            SdfType::ArrowRow(SdfArrowRow { columns: vec![] }),
        );

        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "my-state-value".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_key(&types)
            .expect_err("should error for output type not hashable");

        assert!(res.errors.contains(&ValidationError::new(r#"output type for assign-key type function `my-assign-key` must be hashable, or a reference to a hashable type. found `my-state-value`.
 hashable types: [u8, u16, u32, u64, s8, s16, s32, s64, bool, string, f32, f64]"#)));
    }

    #[test]
    fn test_validate_assign_key_accepts_valid_function() {
        let function = StepInvocation {
            uses: "my-assign-key".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function
            .validate_assign_key(&types())
            .expect("should be valid");
    }

    #[test]
    fn test_validate_assign_timestamp_requires_two_inputs() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "i64".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "i64".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for too few inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-timestamp type function `my-assign-timestamp` should have exactly 2 input type, found 1"
        )));

        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "third-input".to_string(),
                    type_: TypeRef {
                        name: "u8".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for too many inputs");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-timestamp type function `my-assign-timestamp` should have exactly 2 input type, found 3"
        )));
    }

    #[test]
    fn test_validate_assign_timestamp_requires_one_output() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            output: None,
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for missing output");

        assert!(res.errors.contains(&ValidationError::new(
            "assign-timestamp type function `my-assign-timestamp` requires an output type"
        )));
    }

    #[test]
    fn test_validate_assign_timestamp_requires_input_types_are_in_scope() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "foobar".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "s64".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-assign-timestamp` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));

        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "s64".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "foobar".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for input type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-assign-timestamp` has invalid input type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_assign_timestamp_requires_output_type_is_in_scope() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "foobar".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for output type not in scope");

        assert!(res.errors.contains(&ValidationError::new(&format!(
            "function `my-assign-timestamp` has invalid output type, {}",
            &ref_type_error("foobar")
        ))));
    }

    #[test]
    fn test_validate_assign_timestamp_requires_second_input_type_is_s64() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for second input type not s64");

        assert!(res.errors.contains(&ValidationError::new("second input type for assign-timestamp type function `my-assign-timestamp` must be a signed 64-bit int or an alias for one, found: `string`")));
    }

    #[test]
    fn test_validate_assign_timestamp_requires_output_type_is_s64() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "string".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = function
            .validate_assign_timestamp(&types())
            .expect_err("should error for second input type not s64");

        assert!(res.errors.contains(&ValidationError::new("output type for assign-timestamp type function `my-assign-timestamp` must be a signed 64-bit int or an alias for one, found: `string`")));
    }

    #[test]
    fn test_validate_assign_timestamp_accepts_valid_fn() {
        let function = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "other-input".to_string(),
                    type_: TypeRef {
                        name: "i64".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "i64".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        function
            .validate_assign_timestamp(&types())
            .expect("should accept valid function");
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_validate_code_block() {
        let function = StepInvocation {
            uses: "my-code-block".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            code_info: CodeInfo {
                code: Some("fn my_code_block(input: string) -> Result<u8> { 1 }".to_string()),
                lang: CodeLang::Rust,
                extra_deps: vec![],
            },
            ..Default::default()
        };

        function.validate_code().expect("should validate");
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_validate_code_block_checks_syntax() {
        let function = StepInvocation {
            uses: "my-code-block".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            code_info: CodeInfo {
                code: Some("fn my_code_block(input: string) -> Result<u8> {".to_string()),
                lang: CodeLang::Rust,
                extra_deps: vec![],
            },
            ..Default::default()
        };

        let err = function.validate_code().expect_err("should fail");

        assert!(
            err.errors.contains(&ValidationError::new(
               "Failed to parse code. Is this valid Rust syntax for a function?:\n fn my_code_block(input: string) -> Result<u8> {"
            )),
            "{:?}",
            err
        );
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_update_signature() {
        let mut step = StepInvocation {
            uses: "my-step".to_string(),
            inputs: vec![NamedParameter {
                name: "old-name".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            code_info: CodeInfo {
                code: Some(
                    "fn my_code_block(input: MyInput) -> Result<MyOutput> { 1 }".to_string(),
                ),
                lang: CodeLang::Rust,
                extra_deps: vec![],
            },
            ..Default::default()
        };
        step.update_signature_from_code().expect("failed to update");
        assert_eq!(step.uses, "my-code-block");
        assert_eq!(step.inputs.len(), 1);
        assert_eq!(step.inputs[0].name, "input");
        assert_eq!(step.inputs[0].type_.name, "my-input");
        assert!(step.output.is_some());
        assert_eq!(step.output.unwrap().type_.value_type().name, "my-output");
    }

    #[test]
    fn test_wit_interface_map() {
        let step = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::Map);

        let expected_interface =
            "interface my-map-service {\n  my-map: func(input: string) -> result<u8, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_wit_filter_map() {
        let step = StepInvocation {
            uses: "my-filter-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                optional: true,
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::FilterMap);

        let expected_interface = "interface my-filter-map-service {\n  my-filter-map: func(input: string) -> result<option<u8>, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_wit_flat_map() {
        let step = StepInvocation {
            uses: "my-flat-map".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "u8".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::FlatMap);

        let expected_interface = "interface my-flat-map-service {\n  my-flat-map: func(input: string) -> result<list<u8>, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_wit_filter() {
        let step = StepInvocation {
            uses: "my-filter".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "bool".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::Filter);

        let expected_interface = "interface my-filter-service {\n  my-filter: func(input: string) -> result<bool, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_wit_update_state() {
        let step = StepInvocation {
            uses: "my-update".to_string(),
            inputs: vec![NamedParameter {
                name: "input".to_string(),
                type_: TypeRef {
                    name: "string".to_string(),
                },
                optional: false,
                kind: ParameterKind::Value,
            }],
            output: None,
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::UpdateState);

        let expected_interface = "interface my-update-service {\n  my-update: func(input: string) -> result<_, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_wit_assign_timestamp() {
        let step = StepInvocation {
            uses: "my-assign-timestamp".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
                NamedParameter {
                    name: "timestamp".to_string(),
                    type_: TypeRef {
                        name: "s64".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: TypeRef {
                    name: "s64".to_string(),
                }
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::AssignTimestamp);

        let expected_interface = "interface my-assign-timestamp-service {\n  my-assign-timestamp: func(input: string, timestamp: s64) -> result<s64, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }

    #[test]
    fn test_map_with_key() {
        let step = StepInvocation {
            uses: "my-map".to_string(),
            inputs: vec![
                NamedParameter {
                    name: "key".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: true,
                    kind: ParameterKind::Key,
                },
                NamedParameter {
                    name: "input".to_string(),
                    type_: TypeRef {
                        name: "string".to_string(),
                    },
                    optional: false,
                    kind: ParameterKind::Value,
                },
            ],
            output: Some(Parameter {
                type_: crate::wit::metadata::OutputType::KeyValue(SdfKeyValue {
                    key: TypeRef {
                        name: "string".to_string(),
                    },
                    value: TypeRef {
                        name: "u8".to_string(),
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let interface = step.wit_interface(&OperatorType::Map);

        let expected_interface = "interface my-map-service {\n  my-map: func(key: option<string>, input: string) -> result<tuple<option<string>, u8>, string>;\n}\n";

        assert_eq!(
            expected_interface,
            WitInterfaceDisplay(interface).to_string()
        )
    }
}
