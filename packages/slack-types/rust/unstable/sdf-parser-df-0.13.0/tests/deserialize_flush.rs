use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_deserialize_flush() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.1.0
  namespace: my-org
config:
  converter: json
topics:
  my-input-topic:
    name: my-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
  my-output-topic:
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
        id: my-input-topic
    states:
      odd-even-count:
        type: keyed-state
        properties:
          key:
            type: bool        # odd/event
          value:
            type: u32      # count of numbers
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
            uses: double
            inputs:
              - name: value
                type: u64
            output:
              type: u64
      flush:
        uses: aggregate
        inputs: []
        output:
            type: u64
    sinks:
      - type: topic
        id: my-output-topic
"
    );

    parse(&yaml).expect("parse and validate yaml");
}
