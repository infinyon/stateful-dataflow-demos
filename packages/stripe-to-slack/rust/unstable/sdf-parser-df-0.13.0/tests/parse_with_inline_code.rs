use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate_with_inline_code() {
    let yaml = format!(
        "
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.1.0
  namespace: my-org
types:
  sentence:
    type: string
  word-count:
    type: object
    properties:
      word:
        type: string
      count:
        type: u32
# default config
config:
  converter: json
  consumer:
    default_starting_offset:
      value: 0
      position: End
topics:
  sentence:
    name: sentence
    schema:
      value:
        type: string
        converter: raw
  word-count:
    name: word-count
    schema:
      value:
        type: word-count

services:
  word-count-processing:
    sources:
      - type: topic
        id: sentence
    states:
      odd-even-count:
        type: keyed-state
        properties:
          key:
            type: string
          value:
            type: u32
    transforms:
      - operator: flat-map
        run: |
          fn split_sequence(sentence: String) -> Result<Vec<String>, String> {{
            Ok(sentence.split_whitespace().map(String::from).collect())
          }}
    window:
      tumbling:
        duration: 10s
      assign-timestamp:
        run: |
          fn assign_timestamp_fn(value: String, event_time: i64) -> Result<i64, String> {{
            todo!()
          }}

      partition:
        assign-key:
          run: |
            fn assign_key_word(word: String) -> Result<String, String> {{
              Ok(word.to_lowercase().chars().filter(|c| c.is_alphanumeric()).collect())
            }}
        transforms:
          - operator: map
            run: |
              fn count_word(word: String) -> Result<WordCount, String> {{
                let counter = count_per_word();
                let value = counter.increment(1);
                Ok(WordCount {{
                  word: word,
                  count: value as u32,
                }})
              }}
    sinks:
      - type: topic
        id: word-count
  "
    );
    let config = parse(&yaml).expect("parse yaml");

    assert_eq!(config.name(), "my-service");
    assert_eq!(config.version(), "0.1.0");
}
