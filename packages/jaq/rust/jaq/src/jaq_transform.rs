use sdfg::Result;
use sdfg::sdf;
use sdfg::anyhow::Context;
use crate::bindings::examples::jaq::types::Bytes;

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, RcIter};
use jaq_json::Val;
use serde_json::Value;

static FILTER: OnceLock<_> = OnceLock::new();

/// Event Processing function
#[sdf(fn_name = "jaq-transform")]
pub(crate) fn jaq_transform(input: Bytes) -> Result<Option<Bytes>> {
    let filter_rules = get_param("jaq-filter").context("missing parameter")?;
    run_jaq_transform(input, &filter_rules)
}

/// JAQ transform function
fn run_jaq_transform(input: Bytes, filter_rules: &str) -> Result<Option<Bytes>> {
    let filter_file = File {
        code: filter_rules,
        path: (),
    };

    let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let arena = Arena::default();
    let modules = loader.load(&arena, filter_file).map_err(|err| {
        let output_err = err
            .iter()
            .map(|err| format!("{:#?}", err))
            .collect::<Vec<String>>()
            .join("\n");
        sdfg::anyhow::anyhow!(output_err)
    })?;

    let compiler = Compiler::default().with_funs(jaq_std::funs().chain(jaq_json::funs()));
    let filter = compiler
        .compile(modules)
        .map_err(|err| sdfg::anyhow::anyhow!(format!("{:#?}", err)))?;

    let json: Value = serde_json::from_slice(input.as_ref())?;

    let inputs = RcIter::new(core::iter::empty());
    let mut out = filter.run((Ctx::new([], &inputs), Val::from(json)));
    let mut out_json: Vec<Value> = vec![];
    loop {
        match out.next() {
            Some(Ok(val)) => {
                out_json.push(val.into());
            }
            Some(Err(err)) => return Err(sdfg::anyhow::anyhow!(format!("{:#?}", err))),
            None => {
                break;
            }
        }
    }

    match out_json.len() {
        0 => Ok(None),
        1 => {
            if out_json[0].is_null() {
                Ok(None)
            } else {
                let buf = serde_json::to_vec(&out_json[0])?;
                Ok(Some(buf.into()))
            }
        }
        _ => {
            let buf = serde_json::to_vec(&out_json)?;
            Ok(Some(buf.into()))
        }
    }
}

fn get_fileter() -> &'static Filter {
    FILTER.get_or_init(|| {
        get_param("jaq-filter").context("missing parameter")
    })
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_local() {
        let creatures = r#"[
            { "name": "Sammy", "type": "shark", "clams": 5 },
            { "name": "Bubbles", "type": "orca", "clams": 3 },
            { "name": "Splish", "type": "dolphin", "clams": 2 },
            { "name": "Splash", "type": "dolphin", "clams": 2 }
        ]"#.as_bytes().to_vec();
        let filter = ".[] | .name";

        let raw_result = run_jaq_transform(creatures, filter)
            .expect("cannot transform")
            .expect("must be some");
        let result = std::str::from_utf8(&raw_result).expect("cannot convert to str");
        println!("{}", result);

        let out = serde_json::from_slice::<Value>(result.as_ref()).expect("failed");
        assert_eq!(out, json!(["Sammy", "Bubbles", "Splish", "Splash"]));
    }

    #[test]
    fn test_event_match() {
        let input_file: Vec<u8> = std::fs::read("../../sample-data/event1.json")
            .expect("cannot read event1 - input file");
        let filter = std::fs::read_to_string("../../sample-data/filters/invoice-filter.jq")
            .expect("cannot read filter - input file");
        let output_file: Vec<u8> = std::fs::read("../../sample-data/output/event1.json")
            .expect("cannot read event1 - output file");

        let raw_result = run_jaq_transform(input_file, &filter)
            .expect("cannot transform")
            .expect("must be some");
        let result_str = std::str::from_utf8(&raw_result).expect("convert result to str");
        let result_value = serde_json::from_slice::<Value>(result_str.as_ref()).expect("convert result to value");
        println!("{}", result_value);

        let output_str = std::str::from_utf8(&output_file).expect("convert output to str");
        let output_value = serde_json::from_slice::<Value>(output_str.as_ref()).expect("convert output to value");
        println!("{}", output_value);

        assert_eq!(result_value, output_value);
    }

    #[test]
    fn test_event_no_match() {
        let input_file: Vec<u8> = std::fs::read("../../sample-data/event2.json")
            .expect("cannot read event2 - input file");
        let filter = std::fs::read_to_string("../../sample-data/filters/invoice-filter.jq")
            .expect("cannot read filter - input file");

        let raw_result = run_jaq_transform(input_file, &filter).expect("cannot transform");
        assert!(raw_result.is_none());
    }  
}