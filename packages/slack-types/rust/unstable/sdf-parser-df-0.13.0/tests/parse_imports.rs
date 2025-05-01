use sdf_parser_df::parse;

#[test]
fn test_parse_imports() {
    let yaml = "
apiVersion: 0.5.0
meta:
  name: hello-dataflow
  version: 0.1.0
  namespace: example

imports:
  - pkg: example/my-package@0.1.0
    functions:
      - name: my-hello-fn
        alias: hello-fn
    path: test/bank-update

topics:
  sentences:
    schema:
      value:
        type: string
        converter: raw
  first-word:
    schema:
      value:
        type: string
        converter: raw

services:
  hello-service:
    sources:
      - type: topic
        id: sentences

    transforms:
      - operator: filter-map
        uses: hello-fn

    sinks:
      - type: topic
        id: first-word
"
    .to_string();

    let config = parse(&yaml).expect("parse yaml");
    let import = config.imports().first().expect("Should have an import");
    assert_eq!(import.package.namespace, "example");
    assert_eq!(import.package.name, "my-package");
    assert_eq!(import.package.version, "0.1.0");
    assert_eq!(import.functions[0].name, "my-hello-fn");
    assert_eq!(import.functions[0].alias, Some("hello-fn".to_string()));
}
