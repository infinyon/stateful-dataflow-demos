#[allow(dead_code)]
#[rustfmt::skip]
#[allow(clippy::all)]
pub mod bindings {
    use wit_bindgen::generate;

    generate!({
        world: "row-world",
        path: "wit",
        generate_all,
    });
}

use std::collections::HashMap;

use anyhow::{Result, Context};

use bindings::sdf::row_state::row::{Dvalue, RowValue};
use serde::de::DeserializeOwned;

impl Dvalue {
    fn value(&self) -> Result<serde_json::Value> {
        use serde_json::Value;
        let val = match self {
            Dvalue::String(s) => Value::String(s.clone()),
            Dvalue::U8(i) => Value::Number((*i).into()),
            Dvalue::U16(i) => Value::Number((*i).into()),
            Dvalue::U32(i) => Value::Number((*i).into()),
            Dvalue::U64(i) => Value::Number((*i).into()),
            Dvalue::I8(i) => Value::Number((*i).into()),
            Dvalue::I16(i) => Value::Number((*i).into()),
            Dvalue::I32(i) => Value::Number((*i).into()),
            Dvalue::I64(i) => Value::Number((*i).into()),
            Dvalue::Float32(i) => Value::Number(
                serde_json::Number::from_f64(*i as f64).context("failed to parse f64")?,
            ),
            Dvalue::Float64(i) => {
                Value::Number(serde_json::Number::from_f64(*i).context("failed to parse f64")?)
            }

            Dvalue::Bool(i) => Value::Bool(*i),
            Dvalue::Timestamp(i) => Value::Number((*i).into()),
        };
        Ok(val)
    }
}

/// Serde based serializer/deserializer
#[derive(Default)]
pub struct RowSerde;

impl RowSerde {
    pub fn deserialize_from<T: DeserializeOwned>(row: &RowValue) -> Result<T> {
        let map_values: HashMap<String, serde_json::Value> = row
            .get()
            .into_iter()
            .map(|(key, value)| value.value().map(|value| (key.replace('-', "_"), value)))
            .collect::<Result<_>>()?;
        let value = serde_json::to_value(map_values)?;

        let output: T = serde_json::from_value(value)?;
        Ok(output)
    }
}
