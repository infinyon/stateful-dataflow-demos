use sdf_parser_df::parse;

#[test]
fn test_bad_import_name() {
    let yaml = "
apiVersion: 0.5.0
meta:
  name: hello-dataflow
  version: 0.1.0
  namespace: example

imports:
  - pkg: example@0.1.0
    functions:
      - name: my-hello-fn
        alias: hello-fn

topics:
  sentences:
    schema:
      value:
        type: string

services:
  hello-service:
    sources:
      - type: topic
        id: sentences

    transforms:
      - operator: filter-map
        uses: hello-fn
"
    .to_string();

    let error = parse(&yaml).unwrap_err();

    assert_eq!(
        format!("{}", error.root_cause()),
        "invalid value: string \"example@0.1.0\", expected a string of the form `<namespace>/<name>@<version>`"
    );
}
