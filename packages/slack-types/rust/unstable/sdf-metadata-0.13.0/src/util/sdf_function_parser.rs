use anyhow::{anyhow, Context, Result};
use sdf_common::render::wit_name_case;
use syn::{punctuated::Punctuated, token::Comma, FnArg, ItemFn, ReturnType, TypePath, TypeTuple};

use sdf_parser_core::config::types::{
    KeyValueProperties, MetadataType, MetadataTypeInner, MetadataTypeTagged,
};

use crate::wit::{
    metadata::{
        NamedParameter as NamedParameterWit, Parameter as ParameterWit,
        ParameterKind as ParameterKindWit,
    },
    operator::CodeLang as CodeLangWit,
};
pub struct SDFFunctionParser;

impl SDFFunctionParser {
    fn parse_output_rust(output: &ReturnType, uses: &str) -> Result<Option<ParameterWit>> {
        let output = match output {
            syn::ReturnType::Type(_, ty) => {
                match &**ty {
                    syn::Type::Path(path) => {
                        let output = &path.path.segments[0];

                        if output.ident != "Result" {
                            return Err(anyhow!(
                                "Invalid output type on function {uses}. It must be a Result type"
                            ));
                        }

                        // type, optional
                        let ty: Option<(MetadataType, bool)> = match &output.arguments {
                            syn::PathArguments::AngleBracketed(args) => match &args.args[0] {
                                syn::GenericArgument::Type(ty) => {
                                    match ty {
                                        syn::Type::Path(path) => {
                                            // if Option or Vec we return inner type
                                            if ["Option", "Vec"].contains(
                                                &path.path.segments[0].ident.to_string().as_str(),
                                            ) {
                                                let optional = path.path.segments[0]
                                                    .ident
                                                    .to_string()
                                                    .as_str()
                                                    == "Option";

                                                match &path.path.segments[0].arguments {
                                            syn::PathArguments::AngleBracketed(args) => {
                                                match &args.args[0] {
                                                    syn::GenericArgument::Type(
                                                        syn::Type::Path(inner_path),
                                                    ) => {
                                                        let  ty = inner_path.path.segments[0]
                                                        .ident
                                                        .to_string()
                                                        .parse()?;
                                                        Some(
                                                        (ty, optional)
                                                    )},
                                                    syn::GenericArgument::Type(syn::Type::Tuple(tpl)) => match Self::parse_tuple_key_value(tpl)? {
                                                        Some(kv) => Some((MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyValue(kv.into())).into(), optional)),
                                                        None =>  {
                                                            return Err(anyhow!(
                                                            "Invalid output type on function {uses}"
                                                        ))
                                                        }
                                                    },
                                                    _ => {
                                                        return Err(anyhow!(
                                                        "Invalid output type on function {uses}"
                                                    ))
                                                    }
                                                }
                                            }
                                            _ => {
                                                return Err(anyhow!(
                                                    "Invalid output type on function {uses}"
                                                ))
                                            }
                                        }
                                            } else {
                                                Some((
                                                    path.path.segments[0]
                                                        .ident
                                                        .to_string()
                                                        .parse()?,
                                                    false,
                                                ))
                                            }
                                        }
                                        syn::Type::Tuple(tpl) => Self::parse_tuple_key_value(tpl)?
                                            .map(|kv| {
                                                (
                                                    MetadataTypeInner::MetadataTypeTagged(
                                                        MetadataTypeTagged::KeyValue(kv.into()),
                                                    )
                                                    .into(),
                                                    false,
                                                )
                                            }),
                                        _ => {
                                            return Err(anyhow!(
                                                "Invalid output type on function  {uses}",
                                            ))
                                        }
                                    }
                                }
                                _ => return Err(anyhow!("Invalid output type on function {uses}")),
                            },
                            _ => return Err(anyhow!("Invalid output type on function {uses}")),
                        };

                        let wit_ty = ty
                            .map(|(t, optional)| t.try_into().map(|t| (t, optional)))
                            .transpose()?;

                        wit_ty.map(|(ty, optional)| ParameterWit {
                            type_: ty,
                            optional,
                        })
                    }
                    _ => return Err(anyhow!("Invalid error type on output of function {uses}")),
                }
            }
            _ => None,
        };

        Ok(output)
    }

    fn parse_tuple_key_value(ty_tuple: &TypeTuple) -> Result<Option<KeyValueProperties>> {
        match ty_tuple.elems.len() {
            0 => Ok(None),
            1 => Err(anyhow!("Invalid tuple length")),
            2 => {
                let key = match &ty_tuple.elems[0] {
                    syn::Type::Path(path) => {
                        if path.path.segments[0].ident != "Option" {
                            return Err(anyhow!(
                                "Invalid tuple type. First element (key) should be Option"
                            ));
                        }
                        match &path.path.segments[0].arguments {
                            syn::PathArguments::AngleBracketed(args) => match &args.args[0] {
                                syn::GenericArgument::Type(syn::Type::Path(path)) => {
                                    path.path.segments[0].ident.to_string()
                                }
                                _ => return Err(anyhow!("Invalid tuple type")),
                            },
                            _ => return Err(anyhow!("Invalid tuple type")),
                        }
                    }
                    _ => return Err(anyhow!("Invalid tuple type")),
                };

                let value = match &ty_tuple.elems[1] {
                    syn::Type::Path(path) => {
                        if path.path.segments[0].ident == "Option" {
                            return Err(anyhow!(
                                "Invalid tuple type. Second element (value) should not be Option"
                            ));
                        }
                        path.path.segments[0].ident.to_string()
                    }
                    _ => return Err(anyhow!("Invalid tuple type")),
                };

                let key = Box::new(key.parse()?);
                let value = Box::new(value.parse()?);

                Ok(Some(KeyValueProperties { key, value }))
            }
            _ => Err(anyhow!("Invalid tuple length")),
        }
    }

    fn parse_input_rust(
        input: &Punctuated<FnArg, Comma>,
        uses: &str,
    ) -> Result<Vec<NamedParameterWit>> {
        let inputs = input
            .iter()
            .map(|input| {
                let name = match input {
                    syn::FnArg::Typed(pat_type) => match &*pat_type.pat {
                        syn::Pat::Ident(ident) => wit_name_case(&ident.ident.to_string()),
                        _ => return Err(anyhow!("Invalid input on function {uses}")),
                    },
                    _ => return Err(anyhow!("Invalid input on function {uses}")),
                };

                let (ty, optional) = match input {
                    syn::FnArg::Typed(pat_type) => match &pat_type.ty.as_ref() {
                        syn::Type::Path(path) => Self::extract_type(uses, path)?,
                        _ => return Err(anyhow!("Invalid input type on function {uses}")),
                    },
                    _ => return Err(anyhow!("Invalid input type on function {uses}")),
                };

                // assume that optional parameters are key
                let kind = if optional {
                    ParameterKindWit::Key
                } else {
                    ParameterKindWit::Value
                };

                let ty: MetadataType = ty.parse()?;

                Ok(NamedParameterWit {
                    name,
                    type_: ty.into(),
                    optional,
                    kind,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(inputs)
    }

    fn extract_type(uses: &str, path: &TypePath) -> Result<(String, bool)> {
        let (ty, optional) = if path.path.segments[0].ident == "Vec" {
            // we check if the inner type is u8, otherwise is a failure
            match &path.path.segments[0].arguments {
                syn::PathArguments::AngleBracketed(args) => match &args.args[0] {
                    syn::GenericArgument::Type(syn::Type::Path(path)) => {
                        if path.path.segments[0].ident == "u8" {
                            ("bytes".to_string(), false)
                        } else {
                            return Err(anyhow!("Invalid input type on function {uses}"));
                        }
                    }
                    _ => return Err(anyhow!("Invalid input type on function {uses}")),
                },
                _ => return Err(anyhow!("Invalid input type on function {uses}")),
            }
        } else if path.path.segments[0].ident == "Option" {
            match &path.path.segments[0].arguments {
                syn::PathArguments::AngleBracketed(args) => {
                    if args.args.len() != 1 {
                        return Err(anyhow!("Invalid input type on function {uses}"));
                    }
                    match &args.args[0] {
                        syn::GenericArgument::Type(syn::Type::Path(path)) => {
                            (Self::extract_type(uses, path)?.0, true)
                        }
                        _ => return Err(anyhow!("Invalid input type on function {uses}")),
                    }
                }
                _ => return Err(anyhow!("Invalid input type on function {uses}")),
            }
        } else {
            (path.path.segments[0].ident.to_string(), false)
        };

        Ok((ty, optional))
    }

    /// Parse code from str, based on language.
    /// Extracted values are: function name, inputs and output.
    /// Notice that for the output we assume that it is a Result, so we return the inner type.
    /// When the inner type is Option or Vector we return the inner type of them.
    pub(crate) fn parse(
        lang: &CodeLangWit,
        code: &str,
    ) -> Result<(String, Vec<NamedParameterWit>, Option<ParameterWit>)> {
        match lang {
            CodeLangWit::Rust => {
                let parsed_fn: ItemFn = syn::parse_str(code).context(format!(
                    "Failed to parse code. Is this valid Rust syntax for a function?:\n {}",
                    code
                ))?;

                let uses = wit_name_case(&parsed_fn.sig.ident.to_string());

                if parsed_fn.sig.asyncness.is_some() {
                    return Err(anyhow!("{uses} function is async"));
                }

                let inputs = Self::parse_input_rust(&parsed_fn.sig.inputs, &uses)?;

                let output = Self::parse_output_rust(&parsed_fn.sig.output, &uses)?;

                Ok((uses, inputs, output))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use sdf_parser_core::config::transform::{code::Code, Lang};

    use super::*;

    #[test]
    fn parse_code_simple_map() {
        let code = r#"
        fn my_map(my_input: String) -> Result<String, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "my-map");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "my-input");
        assert_eq!(inputs[0].type_.name, "string");
        assert_eq!(output.unwrap().type_.value_type_name(), "string");
    }

    #[test]
    fn parse_code_custom_type() {
        let code = r#"
        fn my_map(word_count: WordCode) -> Result<i64, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "my-map");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "word-count");
        assert_eq!(inputs[0].type_.name, "word-code");
        assert_eq!(output.unwrap().type_.value_type_name(), "s64");
    }

    #[test]
    fn parse_filter() {
        let code = r#"
        fn my_filter(word: String) -> Result<bool, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "my-filter");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "word");
        assert_eq!(inputs[0].type_.name, "string");
        assert_eq!(output.unwrap().type_.value_type_name(), "bool");
    }

    #[test]
    fn parse_assign_timestamp() {
        let code = r#"
        fn assign_timestamp_fn(value: String, event_time: i64) -> Result<i64, String> {
            // println!("Using {} as event_time", event_time);
            Ok(event_time)
        }
            "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "assign-timestamp-fn");
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "value");
        assert_eq!(inputs[0].type_.name, "string");
        assert_eq!(inputs[1].name, "event-time");
        assert_eq!(inputs[1].type_.name, "s64");
        assert_eq!(output.unwrap().type_.value_type_name(), "s64");
    }
    #[test]
    fn parse_filter_map() {
        let code = r#"
        fn my_filter_map(word_count: WordCode) -> Result<Option<i64>, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");

        assert_eq!(uses, "my-filter-map");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "word-count");
        assert_eq!(inputs[0].type_.name, "word-code");
        assert_eq!(output.unwrap().type_.value_type_name(), "s64");
    }

    #[test]
    fn parse_flat_map() {
        let code = r#"
        fn my_flat_map(sentences: String) -> Result<Vec<String>, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "my-flat-map");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "sentences");
        assert_eq!(inputs[0].type_.name, "string");
        assert_eq!(output.unwrap().type_.value_type_name(), "string");
    }

    #[test]
    fn test_return_void() {
        let code = r#"
        fn my_process(word_count: WordCode) -> Result<(), String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let (uses, inputs, output) =
            SDFFunctionParser::parse(&code.lang.into(), &code.run).expect("parse code");
        assert_eq!(uses, "my-process");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "word-count");
        assert_eq!(inputs[0].type_.name, "word-code");
        assert!(output.is_none(), "output should be none");
    }

    #[test]
    fn test_not_accept_async() {
        let code = r#"
        async fn my_filter(word_count: WordCode) -> Result<String, String> {
            println!("Hello, world!");
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let err = SDFFunctionParser::parse(&code.lang.into(), &code.run).expect_err("parse code");

        assert_eq!(err.to_string(), "my-filter function is async");
    }

    #[test]
    fn test_not_result_output() {
        let code = r#"
        fn my_map(word_count: WordCode) -> String {
            "hello".to_string()
        }
        "#;

        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let err = SDFFunctionParser::parse(&code.lang.into(), &code.run).expect_err("parse code");

        assert_eq!(
            err.to_string(),
            "Invalid output type on function my-map. It must be a Result type"
        );
    }

    #[test]
    fn parse_invalid_input() {
        let code = r#"
        fn my_map(&self) -> Result<String, String> {
            println!("Hello, world!");
        }
        "#;
        let code = Code {
            export_name: None,
            lang: Lang::Rust,
            state_imports: vec![],
            run: code.to_string(),
            dependencies: vec![],
        };

        let err = SDFFunctionParser::parse(&code.lang.into(), &code.run).expect_err("parse code");
        assert_eq!(err.to_string(), "Invalid input on function my-map");
    }
}
