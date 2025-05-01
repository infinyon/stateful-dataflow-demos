use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.2.0
  namespace: my-org
topics:
  my-topic:
    name: my-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
        converter: json
  my-output:
    name: double-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
        converter: json
config:
  converter: json
services:
  basic_operation:
    sources:
      - type: topic
        id: my-topic
    states:
      odd-even-count:
        type: keyed-state
        properties:
          key:
            type: bool        # odd/event
          value:
            type: u32      # count of numbers
    partition:
      assign-key:
          uses: assign-key-odd-even
          inputs:
            - name: value
              type: u64
          output:
            type: bool
      transforms:
        - operator: map
          uses: doubles
          inputs:
            - name: value
              type: u64
          output:
            type: u64
    sinks:
      - type: topic
        id: my-output
types:
  foo:
    type: string
  my-kv:
    type: keyed-state
    properties:
      key:
        type: foo
      value:
        type: string
"
    );

    let _config = parse(&yaml).expect("parse yaml");
}

#[test]
fn test_f64() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.2.0
  namespace: my-org
topics:
  my-topic:
    name: my-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: f64
        converter: json
  my-output:
    name: double-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: f32
        converter: json
config:
  converter: json
services:
  basic_operation:
    sources:
      - type: topic
        id: my-topic
    states:
      odd-even-count:
        type: keyed-state
        properties:
          key:
            type: bool        # odd/event
          value:
            type: u32      # count of numbers
    partition:
      assign-key:
          uses: assign-key-odd-even
          inputs:
            - name: value
              type: u64
          output:
            type: bool
      transforms:
        - operator: map
          uses: doubles
          inputs:
            - name: value
              type: u64
          output:
            type: u64
    sinks:
      - type: topic
        id: my-output
types:
  foo:
    type: string
  my-kv:
    type: keyed-state
    properties:
      key:
        type: foo
      value:
        type: string
"
    );

    let _config = parse(&yaml).expect("parse yaml");
}
