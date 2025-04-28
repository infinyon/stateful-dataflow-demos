use sdf_parser_df::parse;
use sdf_parser_core::config::SERVICE_DEFINITION_CONFIG_STABLE_VERSION;

#[test]
fn test_validate_no_topic_name() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.1.0
  namespace: my-org
topics:
  my-topic:
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
        converter: json
  my-output:
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
    parse(&yaml).expect("parse yaml");
}
