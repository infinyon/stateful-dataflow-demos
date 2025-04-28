use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate_topic_steps_with_aliases() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: wordcount-current
  version: 0.1.0
  namespace: my-org
types:
  json-text:
    type: object
    properties:
      text:
        type: string
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
topics:
  first-source:
    schema:
      value:
        type: sentence
        converter: raw
  second-source:
    schema:
      value:
        type: json-text
  word-count:
    schema:
      value:
        type: word-count

  word-count-a-words:
    schema:
      value:
        type: word-count
  word-count-value:
    schema:
      value:
        type: u32

services:
  word-count-processing:
    sources:
      - type: topic
        id: first-source
      - type: topic
        id: second-source
        steps:
          - operator: map
            run: |
              fn get_inner_text(json_text: JsonText) -> Result<String, String> {{
                Ok(json_text.text)
              }}
    states:
      count-per-word:
        type: keyed-state
        properties:
          key:
            type: string
          value:
            type: u32
    transforms:
      - operator: flat-map
        run: |
          fn split_sequence(sentence: Sentence) -> Result<Vec<String>, String> {{
            Ok(sentence.split_whitespace().map(String::from).collect())
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
      - type: topic
        id: word-count-a-words
        steps:
          - operator: filter
            run: |
              fn filter_a_words(word: WordCount) -> Result<bool, String> {{
                Ok(word.word.starts_with(\"a\"))
              }}
      - type: topic
        id: word-count-value
        steps:
          - operator: map
            run: |
              fn only_output_count(word: WordCount) -> Result<u32, String> {{
                Ok(word.count)
              }}
"
    );

    parse(&yaml).expect("parse yaml");
}
