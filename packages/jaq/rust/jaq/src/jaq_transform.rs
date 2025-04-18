use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::jaq_types::types::Bytes;

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, RcIter};
use jaq_json::Val;
use serde_json::Value;

use crate::filter_rules::JAQ_FILTER;

#[sdf(fn_name = "jaq-transform")]
pub(crate) fn jaq_transform(input: Bytes) -> Result<Bytes> {
    run_jaq_transform(input, JAQ_FILTER)
}

/// JAQ transform function
fn run_jaq_transform(input: Bytes, filter_rules: &str) -> Result<Bytes> {
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

    if out_json.len() == 1 {
        Ok(serde_json::to_vec(&out_json[0])?.into())
    } else {
        Ok(serde_json::to_vec(&out_json)?.into())
    }
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
        let filter_rules = ".[] | .name";

        let raw_result = run_jaq_transform(creatures, filter_rules).expect("cannot transform");
        let result = std::str::from_utf8(&raw_result).expect("cannot convert to str");
        println!("{}", result);

        let out = serde_json::from_slice::<Value>(result.as_ref()).expect("failed");
        assert_eq!(out, json!(["Sammy", "Bubbles", "Splish", "Splash"]));
    }

    #[test]
    fn test_file() {
        let input_file: Vec<u8> = std::fs::read("../../sample-data/event1.json")
            .expect("cannot read event1 - input file");
        let output_file: Vec<u8> = std::fs::read("../../sample-data/output/event1.json")
            .expect("cannot read event1 - output file");

        let raw_result = run_jaq_transform(input_file, JAQ_FILTER).expect("transform");
        let resul_str = std::str::from_utf8(&raw_result).expect("convert result to str");
        let result_value = serde_json::from_slice::<Value>(resul_str.as_ref()).expect("convert result to value");
        println!("{}", result_value);

        let output_str = std::str::from_utf8(&output_file).expect("convert output to str");
        let output_value = serde_json::from_slice::<Value>(output_str.as_ref()).expect("convert output to value");
        println!("{}", output_value);

        assert_eq!(result_value, output_value);
    }

}