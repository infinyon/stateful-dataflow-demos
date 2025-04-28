use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate_topic_steps() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.1.0
  namespace: my-org
topics:
  my-topic-u32:
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u32
        converter: json
  my-topic-i32:
    schema:
      key:
        type: string
        converter: raw
      value:
        type: i32
        converter: json
  my-topic-i64:
    schema:
      key:
        type: string
        converter: raw
      value:
        type: i64
        converter: json
  my-output:
    name: double-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: i64
        converter: json
  my-output-mapped:
    name: double-number-as-string
    schema:
      value:
        type: string
        converter: json
services:
  basic_operation:
    sources:
      - type: topic
        id: my-topic-u32
        steps:
          - operator: map
            uses: u32-transform
            inputs:
              - name: value
                type: u32
            output:
                type: i64
      - type: topic
        id: my-topic-i32
        steps:
          - operator: map
            uses: i32-transform
            inputs:
              - name: value
                type: i32
            output:
                type: i64
      - type: topic
        id: my-topic-i64
    transforms:
      - operator: map
        uses: double
        inputs:
          - name: value
            type: i64
        output:
          type: i64
    sinks:
      - type: topic
        id: my-output
      - type: topic
        id: my-output-mapped
        steps:
          - operator: map
            uses: i64-to-string
            inputs:
              - name: value
                type: i64
            output:
                type: string
"
    );

    parse(&yaml).expect("parse yaml");
}
