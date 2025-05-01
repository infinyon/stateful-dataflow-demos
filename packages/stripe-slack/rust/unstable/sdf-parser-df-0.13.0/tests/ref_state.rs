use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_ref_state() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.1.0
  namespace: my-org
types:
  timestamp:
     type: s64
topics:
  my-topic:
    name: my-topic
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
        converter: json
  my-second-topic:
    name: my-second-topic
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
services:
  basic_operation:
    sources:
      - type: topic
        id: my-topic
    states:
      my-state:
        type: keyed-state
        properties:
          key:
            type: string
          value:
            type: u32

    window:
      tumbling:
        duration: 60s
        offset: 15s
      assign-timestamp:
        uses: assign-timestamp-fn
        inputs:
          - name: value
            type: u64
          - name: event_time
            type: timestamp
        output:
          type: timestamp

      transforms:
        - operator: map
          uses: double
          inputs:
            - name: value
              type: u64
          output:
              type: u64
    sinks:
      - type: topic
        id: my-output
  complex_operation:
    sources:
      - type: topic
        id: my-second-topic
    states:
      my-state:
        from: basic_operation.my-state
    transforms:
      - operator: map
        uses: double
        inputs:
          - name: value
            type: u64
        output:
          type: u64
    sinks:
      - type: topic
        id: my-output

"
    );

    let config = parse(&yaml).expect("parse and validate yaml");
    assert_eq!(config.name(), "my-service");
    assert_eq!(config.version(), "0.1.0");
}
