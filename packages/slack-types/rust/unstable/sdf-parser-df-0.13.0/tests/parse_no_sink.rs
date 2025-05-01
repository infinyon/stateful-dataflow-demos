use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate_no_sink() {
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
     type: i64
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
services:
  basic_operation:
    sources:
      - type: topic
        id: my-topic
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
"
    );

    parse(&yaml).expect("should work");
}
