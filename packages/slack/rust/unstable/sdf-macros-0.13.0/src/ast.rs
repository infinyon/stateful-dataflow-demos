use proc_macro2::Span;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    token, Error, Ident, ItemFn, LitStr, Path, Result, Token, Type,
};

use sdf_common::render::rust_type_case;

#[derive(Copy, Clone, Default)]
pub(crate) enum SdfOperatorKind {
    #[default]
    Filter,
    Map,
    FilterMap,
    FlatMap,
    AssignKey,
    AssignTimestamp,
    Aggregate,
    UpdateState,
}

impl SdfOperatorKind {
    /// Expected possible input lengths for each operator
    /// some operators can have multiple input lengths
    /// due to the key argument could be present or not
    fn expected_input_length(&self) -> Vec<usize> {
        match self {
            Self::Filter => vec![1, 2],
            Self::Map => vec![1, 2],
            Self::FilterMap => vec![1, 2],
            Self::FlatMap => vec![1, 2],
            Self::AssignKey => vec![1, 2],
            Self::AssignTimestamp => vec![2, 3],
            Self::Aggregate => vec![0],
            Self::UpdateState => vec![1, 2],
        }
    }

    fn validate_input(&self, func: &ItemFn) -> Result<()> {
        if !self
            .expected_input_length()
            .contains(&func.sig.inputs.len())
        {
            return Err(Error::new(
                func.sig.inputs.span(),
                format!(
                    "Expected {} input arguments. Found {}",
                    self.expected_input_length()
                        .iter()
                        .map(|l| l.to_string())
                        .collect::<Vec<_>>()
                        .join(" or "),
                    func.sig.inputs.len()
                ),
            ));
        }

        if let Self::AssignTimestamp = self {
            let last_arg = func.sig.inputs.iter().next_back().unwrap();
            if let syn::FnArg::Typed(arg) = last_arg {
                if let syn::Type::Path(path) = &*arg.ty {
                    if path.path.is_ident("i64") {
                        return Ok(());
                    }
                }
                return Err(Error::new(
                    last_arg.span(),
                    "Expected i64 argument for second argument of assign_timestamp",
                ));
            } else {
                return Err(Error::new(
                    last_arg.span(),
                    "Expected i64 argument for second argument of assign_timestamp",
                ));
            }
        }

        Ok(())
    }

    fn validate_output(&self, return_type: &Type) -> Result<()> {
        match self {
            // Check if the return type is Result<bool, _>
            Self::Filter => {
                if let syn::Type::Path(path) = return_type {
                    if path.path.is_ident("bool") {
                        return Ok(());
                    }
                }

                Err(Error::new(
                    return_type.span(),
                    "Expected bool output for filter operator",
                ))
            }
            Self::Map => {
                match return_type {
                    syn::Type::Path(path) => {
                        if path.path.segments.len() == 1 && path.path.segments[0].ident == "Option"
                        {
                            return Err(Error::new(
                            return_type.span(),
                            "Invalid option output type for map operator, maybe you meant filter_map?",
                        ));
                        }
                        return Ok(());
                    }
                    syn::Type::Tuple(tuple) => {
                        if tuple.elems.len() == 2 {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for map operator",
                ))
            }
            Self::FilterMap => {
                if let syn::Type::Path(path) = return_type {
                    if path.path.segments.len() == 1 && path.path.segments[0].ident == "Option" {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for filter_map operator. It must be an Option<_>",
                ))
            }
            Self::FlatMap => {
                if let syn::Type::Path(path) = return_type {
                    if path.path.segments.len() == 1 && path.path.segments[0].ident == "Vec" {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for flat_map operator. It must be a Vec<_>",
                ))
            }
            Self::UpdateState => {
                // No output type expected
                if let syn::Type::Tuple(tuple) = return_type {
                    if tuple.elems.is_empty() {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for update_state operator. It must be an unit type `()`",
                ))
            }
            Self::AssignKey => {
                if let syn::Type::Path(_) = return_type {
                    return Ok(());
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for assign key operator",
                ))
            }
            Self::AssignTimestamp => {
                if let syn::Type::Path(path) = return_type {
                    if path.path.is_ident("i64") {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid output type for assign_timestamp operator. It must be an i64",
                ))
            }
            Self::Aggregate => {
                match return_type {
                    syn::Type::Path(path) => {
                        if path.path.segments.len() == 1 && path.path.segments[0].ident == "Option"
                        {
                            return Err(Error::new(
                                return_type.span(),
                                "Invalid option output type for aggregate operator",
                            ));
                        }

                        return Ok(());
                    }
                    syn::Type::Tuple(tuple) => {
                        if tuple.elems.len() == 2 {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
                Err(Error::new(
                    return_type.span(),
                    "Invalid option output type for aggregate operator",
                ))
            }
        }
    }
}

pub struct SdfOperatorFn<'a> {
    pub name: &'a Ident,
    pub func: &'a ItemFn,
    pub input_types: Vec<syn::Type>,
    pub output_type: syn::Type,
}

impl<'a> SdfOperatorFn<'a> {
    pub fn from_ast(func: &'a ItemFn, kind: Option<SdfOperatorKind>) -> Result<Self> {
        if func.sig.asyncness.is_some() {
            return Err(Error::new(func.span(), "Sdf function must not be async"));
        }
        let name = &func.sig.ident;

        let input_types = func
            .sig
            .inputs
            .iter()
            .map(|arg| {
                if let syn::FnArg::Typed(arg) = arg {
                    Ok((*arg.ty).clone())
                } else {
                    Err(Error::new(
                        arg.span(),
                        "Expected typed argument, found self",
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let output_type = match &func.sig.output {
            syn::ReturnType::Type(_, return_type) => {
                if let syn::Type::Path(path) = &**return_type {
                    if path.path.segments.len() == 1 && path.path.segments[0].ident == "Result" {
                        if let syn::PathArguments::AngleBracketed(args) =
                            &path.path.segments[0].arguments
                        {
                            if args.args.len() <= 2 && !args.args.is_empty() {
                                if let syn::GenericArgument::Type(ty) = &args.args[0] {
                                    ty.clone()
                                } else {
                                    return Err(Error::new(
                                        return_type.span(),
                                        "Invalid output type. Must be a Result<_, _>",
                                    ));
                                }
                            } else {
                                return Err(Error::new(
                                    return_type.span(),
                                    "Invalid output type. Must be a Result<_, _>",
                                ));
                            }
                        } else {
                            return Err(Error::new(
                                return_type.span(),
                                "Invalid output type. Must be a Result<_, _>",
                            ));
                        }
                    } else {
                        return Err(Error::new(
                            return_type.span(),
                            "Invalid output type. Must be a Result<_, _>",
                        ));
                    }
                } else {
                    return Err(Error::new(
                        return_type.span(),
                        "Invalid output type. Must be a Result<_, _>",
                    ));
                }
            }
            syn::ReturnType::Default => {
                // Expected Result as output
                return Err(Error::new(
                    func.sig.output.span(),
                    "Expected return type Result<_, _>",
                ));
            }
        };

        if let Some(kind) = kind {
            kind.validate_input(func)?;
            kind.validate_output(&output_type)?;
        }

        Ok(Self {
            input_types,
            output_type,
            name,
            func,
        })
    }
}

pub struct State {
    pub name: String,
    pub ty: StateType,
    pub update_fn: Option<syn::ExprBlock>,
}

impl State {
    pub fn const_name(&self) -> Ident {
        let upper_case_name = self.name.to_uppercase();
        create_ident(&upper_case_name)
    }

    pub fn state_name(&self) -> Ident {
        create_ident(&self.name)
    }

    pub fn state_name_str(&self) -> &str {
        &self.name
    }

    pub fn item_value_type(&self) -> Ident {
        match self.ty {
            StateType::I32 => create_ident("i32"),
            StateType::Row => {
                let item_value = format!("{}-item-value", self.name);
                let rust_type = rust_type_case(&item_value);
                create_ident(&rust_type)
            }
            StateType::Table | StateType::ListI32 => {
                let rust_type = rust_type_case(&self.name);
                create_ident(&rust_type)
            }
        }
    }

    pub fn type_name(&self) -> Ident {
        let rust_type = rust_type_case(&self.name);
        create_ident(&rust_type)
    }

    pub fn wrapper_type(&self) -> Ident {
        let wrapper = format!("{}-wrapper", self.name);
        let rust_type = rust_type_case(&wrapper);
        create_ident(&rust_type)
    }
}

impl Parse for State {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let call_site = Span::call_site();

        if input.peek(token::Paren) {
            let content;
            syn::parenthesized!(content in input);
            let state_fields = Punctuated::<OptState, Token![,]>::parse_terminated(&content)?;

            let mut name = None;
            let mut update_fn = None;
            let mut ty = None;

            for field in state_fields {
                match field {
                    OptState::StateName(state_name) => {
                        name = Some(state_name.value());
                    }
                    OptState::UpdateFn(update_fn_block) => {
                        update_fn = Some(update_fn_block);
                    }
                    OptState::StateTy(state_type) => {
                        ty = Some(state_type);
                    }
                }
            }

            return Ok(State {
                name: name.ok_or_else(|| {
                    Error::new(
                        call_site,
                        "Missing state name. Try passing the state name with: '#[sdf(state = (name = \"<state_name>\", ty = <state_type>))]'",
                    )
                })?,
                ty: ty.ok_or_else(|| {
                    Error::new(
                        call_site,
                        "Missing state type. Try passing the state type with: '#[sdf(state = (name = \"<state_name>\", ty = <state_type>))]'",
                    )
                })?,
                update_fn,
            });
        }

        Err(Error::new(
            call_site,
            "Invalid state configuration. Expected a tuple with state configurations",
        ))
    }
}

pub enum StateType {
    I32,
    Row,
    Table,
    ListI32,
}

impl Parse for StateType {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let l = input.lookahead1();
        if l.peek(kw::i32) {
            input.parse::<kw::i32>()?;
            Ok(StateType::I32)
        } else if l.peek(kw::row) {
            input.parse::<kw::row>()?;
            Ok(StateType::Row)
        } else if l.peek(kw::table) {
            input.parse::<kw::table>()?;
            Ok(StateType::Table)
        } else if l.peek(kw::list_i32) {
            input.parse::<kw::list_i32>()?;
            Ok(StateType::ListI32)
        } else {
            Err(Error::new(
                input.span(),
                "Invalid state type. Supported types: i32, row, table and list_i32",
            ))
        }
    }
}

pub enum SdfOpConfig {
    Pkg {
        fn_name: String,
    },
    Config {
        path: String,
        package: String,
        world: String,
        namespace: String,
        interface: String,
        bindings_path: Option<Path>,
        operator_kind: SdfOperatorKind,
    },
}
pub struct SdfBindgenConfig {
    pub config: SdfOpConfig,
    pub states: Vec<State>,
}

impl SdfBindgenConfig {
    pub fn kind(&self) -> Option<SdfOperatorKind> {
        match &self.config {
            SdfOpConfig::Config { operator_kind, .. } => Some(*operator_kind),
            // just in order to fill but not used in this case
            SdfOpConfig::Pkg { .. } => None,
        }
    }
}
impl Parse for SdfBindgenConfig {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let call_site = Span::call_site();

        let attributes = Punctuated::<Opt, Token![,]>::parse_terminated(input)?;

        let mut kind = None;
        let mut path = None;
        let mut package = None;
        let mut world = None;
        let mut namespace = None;
        let mut interface = None;
        let mut bindings_path = None;
        let mut states = vec![];
        let mut fn_name = None;
        for attr in attributes {
            match attr {
                Opt::Filter => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }
                    kind = Some(SdfOperatorKind::Filter);
                }
                Opt::Map => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::Map);
                }
                Opt::FilterMap => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::FilterMap);
                }
                Opt::FlatMap => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::FlatMap);
                }
                Opt::AssignKey => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::AssignKey);
                }
                Opt::AssignTimestamp => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::AssignTimestamp);
                }
                Opt::Aggregate => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }

                    kind = Some(SdfOperatorKind::Aggregate);
                }
                Opt::UpdateState => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple operator kinds found. Only one operator kind is allowed",
                        ));
                    }
                    kind = Some(SdfOperatorKind::UpdateState);
                }

                Opt::Path(paths) => {
                    if path.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple paths found. Only one path is allowed",
                        ));
                    }
                    path = Some(paths.value());
                }
                Opt::PackageName(name) => {
                    if package.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple package names found. Only one package name is allowed",
                        ));
                    }
                    package = Some(name.value());
                }
                Opt::World(name) => {
                    if world.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple world names found. Only one world name is allowed",
                        ));
                    }
                    world = Some(name.value());
                }
                Opt::Namespace(name) => {
                    if namespace.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple namespace names found. Only one namespace name is allowed",
                        ));
                    }
                    namespace = Some(name.value());
                }
                Opt::Interface(name) => {
                    if interface.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple interface names found. Only one interface name is allowed",
                        ));
                    }
                    interface = Some(name.value());
                }
                Opt::Bindings(bindings) => {
                    if bindings_path.is_some() {
                        return Err(Error::new(
                            call_site,
                            "Multiple bindings found. Only one bindings is allowed",
                        ));
                    }

                    bindings_path = Some(bindings);
                }
                Opt::State(state) => {
                    states.push(state);
                }
                Opt::FnName(name) => {
                    if kind.is_some() {
                        return Err(Error::new(
                            call_site,
                            "cannot specify fn_name with operator kind",
                        ));
                    }
                    // Ignore fn_name
                    fn_name = Some(name.value());
                }
            }
        }

        let config = match fn_name {
            Some(fn_name) => SdfOpConfig::Pkg { fn_name },
            None => {
                let namespace = namespace
                .ok_or_else(|| {
                    Error::new(
                        call_site,
                        "Missing namespace. Try passing the namespace with: '#[sdf(namespace = \"<namespace>\")]",
                    )
                })?;
                let package = package.ok_or_else(|| {
                    Error::new(
                        call_site,
                        "Missing package. Try passing the package with: '#[sdf(package = \"<package>\")]",
                    )
                })?;
                let interface = interface.unwrap_or_else(|| format!("{}-service", package));
                let world = world.unwrap_or_else(|| format!("{}-world", package));
                let path = path.unwrap_or_else(|| String::from(".wit"));
                let operator_kind = kind.ok_or_else(|| {
                    Error::new(
                        call_site,
                        "Missing operator kind. Try passing the operator kind with: '#[sdf(map|filter|filter_map|flat_map|assign_key|assign_timestamp|aggregate|update_state)]'",
                    )
                })?;

                SdfOpConfig::Config {
                    path,
                    package,
                    world,
                    namespace,
                    interface,
                    bindings_path,
                    operator_kind,
                }
            }
        };

        Ok(Self { config, states })
    }
}

pub(crate) fn create_ident(name: &str) -> Ident {
    Ident::new(&name.replace("-", "_"), Span::call_site())
}

enum Opt {
    Filter,
    Map,
    FilterMap,
    FlatMap,
    AssignKey,
    AssignTimestamp,
    Aggregate,
    UpdateState,
    Path(LitStr),
    PackageName(LitStr),
    World(LitStr),
    Namespace(LitStr),
    Interface(LitStr),
    Bindings(Path),
    State(State),
    FnName(LitStr),
}

impl Parse for Opt {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let l = input.lookahead1();
        if l.peek(kw::map) {
            input.parse::<kw::map>()?;
            Ok(Opt::Map)
        } else if l.peek(kw::filter) {
            input.parse::<kw::filter>()?;
            Ok(Opt::Filter)
        } else if l.peek(kw::filter_map) {
            input.parse::<kw::filter_map>()?;
            Ok(Opt::FilterMap)
        } else if l.peek(kw::flat_map) {
            input.parse::<kw::flat_map>()?;
            Ok(Opt::FlatMap)
        } else if l.peek(kw::assign_key) {
            input.parse::<kw::assign_key>()?;
            Ok(Opt::AssignKey)
        } else if l.peek(kw::assign_timestamp) {
            input.parse::<kw::assign_timestamp>()?;
            Ok(Opt::AssignTimestamp)
        } else if l.peek(kw::aggregate) {
            input.parse::<kw::aggregate>()?;
            Ok(Opt::Aggregate)
        } else if l.peek(kw::update_state) {
            input.parse::<kw::update_state>()?;
            Ok(Opt::UpdateState)
        } else if l.peek(kw::path) {
            input.parse::<kw::path>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::Path(path))
        } else if l.peek(kw::package) {
            input.parse::<kw::package>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::PackageName(path))
        } else if l.peek(kw::world) {
            input.parse::<kw::world>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::World(path))
        } else if l.peek(kw::namespace) {
            input.parse::<kw::namespace>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::Namespace(path))
        } else if l.peek(kw::interface) {
            input.parse::<kw::interface>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::Interface(path))
        } else if l.peek(kw::bindings) {
            input.parse::<kw::bindings>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::Bindings(path))
        } else if l.peek(kw::state) {
            input.parse::<kw::state>()?;
            input.parse::<Token![=]>()?;
            let state = input.parse()?;
            Ok(Opt::State(state))
        } else if l.peek(kw::fn_name) {
            input.parse::<kw::fn_name>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(Opt::FnName(path))
        } else {
            Err(l.error())
        }
    }
}

enum OptState {
    StateName(LitStr),
    UpdateFn(syn::ExprBlock),
    StateTy(StateType),
}

impl Parse for OptState {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let l = input.lookahead1();
        if l.peek(kw::name) {
            input.parse::<kw::name>()?;
            input.parse::<Token![=]>()?;
            let path = input.parse()?;
            Ok(OptState::StateName(path))
        } else if l.peek(kw::update_fn) {
            input.parse::<kw::update_fn>()?;
            input.parse::<Token![=]>()?;
            let block = input.parse()?;
            Ok(OptState::UpdateFn(block))
        } else if l.peek(kw::ty) {
            input.parse::<kw::ty>()?;
            input.parse::<Token![=]>()?;
            let ty = input.parse()?;
            Ok(OptState::StateTy(ty))
        } else {
            Err(l.error())
        }
    }
}

mod kw {
    syn::custom_keyword!(filter);
    syn::custom_keyword!(map);
    syn::custom_keyword!(array_map);
    syn::custom_keyword!(filter_map);
    syn::custom_keyword!(flat_map);
    syn::custom_keyword!(assign_key);
    syn::custom_keyword!(assign_timestamp);
    syn::custom_keyword!(aggregate);
    syn::custom_keyword!(update_state);
    syn::custom_keyword!(path);
    syn::custom_keyword!(package);
    syn::custom_keyword!(world);
    syn::custom_keyword!(namespace);
    syn::custom_keyword!(interface);
    syn::custom_keyword!(bindings);
    syn::custom_keyword!(state);
    syn::custom_keyword!(name);
    syn::custom_keyword!(update_fn);
    syn::custom_keyword!(ty);
    syn::custom_keyword!(i32);
    syn::custom_keyword!(row);
    syn::custom_keyword!(table);
    syn::custom_keyword!(list_i32);
    syn::custom_keyword!(fn_name);
}
