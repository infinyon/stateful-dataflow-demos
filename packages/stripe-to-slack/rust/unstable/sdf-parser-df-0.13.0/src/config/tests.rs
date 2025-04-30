use std::collections::BTreeMap;

use schemars::schema_for;
use sdf_parser_core::config::{
    transform::StateWrapper,
    types::{
        self, ColumnType, EnumType, EnumVariantType, KeyedStateProperties, KeyedStateType,
        ListType, MetadataType, MetadataTypeInner, MetadataTypeTagged, NamedType,
        ObjectPropertyType, ObjectType, OptionType, RowType,
    },
    OffsetWrapper, SerdeConverter,
};

use crate::parse;

use super::*;

#[test]
fn test_deserialize() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}
meta:
  name: my-service
  version: 0.2.0
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
    sinks:
      - type: topic
        id: my-output-topic
"
    );
    let config = parse(&yaml).expect("should validate");

    assert_eq!(config.name(), "my-service");
    assert_eq!(config.version(), "0.2.0");

    let services = config.services().expect("invalid services");
    assert_eq!(
        0,
        services
            .first()
            .as_ref()
            .expect("no services")
            .1
            .transforms
            .len()
    );

    let post_transforms = services
        .first()
        .unwrap()
        .1
        .post_transforms
        .as_ref()
        .unwrap();

    let PostTransforms::Valid(PostTransformsInner::Partition(p)) = post_transforms else {
        panic!("expected partition");
    };
    assert_eq!(p.transforms.len(), 1);
}

#[test]
fn test_deserialize_types() {
    let yaml = format!(
        "
---
apiVersion: {SERVICE_DEFINITION_CONFIG_STABLE_VERSION}

meta:
  name: my-service
  version: 0.2.0
  namespace: my-org
topics:
  my-input-topic:
    name: my-number
    schema:
      key:
        type: string
        converter: raw
      value:
        type: u64
        converter: json
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
        id: my-output-topic
types:
  utc-timestamp:
    type: u64
  email:
    type: string
  decimal:
    type: u64
  price-version:
    type: enum
    oneOf:
      version1:
        type: null
      version2:
        type: null
  usage-credit-id:
    type: object
    properties:
      account-id:
        type: account-id
      start-time:
        type: utc-timestamp
      end-time:
        type: utc-timestamp
      order:
        type: option
        value:
          type: u32
  apply-credit-kv:
    type: keyed-state
    properties:
      key:
        type: credit-id
      value:
        type: apply-credit-event
  usage-rollup:
    type: list
    items:
      type: keyed-state
      properties:
        key:
          type: string
        value:
          type: decimal
  parent:
    type: option
    value:
      type: string
  my-row:
    type: arrow-row
    properties:
      name:
        type: string
      count:
        type: s32
  my-nested-enum:
    type: enum
    oneOf:
      empty:
        type: null
      nested:
        type: object
        properties:
          name:
            type: string
          my-list:
            type: list
            items:
              type: object
              properties:
                name:
                  type: string
                count:
                  type: s32
"
    );

    let config = serde_yaml::from_str::<DataflowDefinitionConfig>(&yaml).expect("parse yaml");
    assert_eq!(config.name(), "my-service");
    assert_eq!(config.version(), "0.2.0");
    let types = config.types().expect("invalid types");
    assert_eq!(types.len(), 10);

    assert_eq!(types["utc-timestamp"], MetadataTypeTagged::U64.into());
    assert_eq!(types["email"], MetadataTypeTagged::String.into());
    assert_eq!(types["decimal"], MetadataTypeTagged::U64.into());
    assert_eq!(
        types["price-version"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Enum(EnumType {
            tagging: Default::default(),
            one_of: vec![
                (
                    "version1".to_string(),
                    EnumVariantType {
                        ty: Some(MetadataTypeInner::None(Default::default()).into()),
                        serde: Default::default(),
                    }
                ),
                (
                    "version2".to_string(),
                    EnumVariantType {
                        ty: Some(MetadataTypeInner::None(Default::default()).into()),
                        serde: Default::default(),
                    }
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
        }))
        .into()
    );
    assert_eq!(
        types["usage-credit-id"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Object(ObjectType {
            properties: vec![
                (
                    "account-id".to_string(),
                    ObjectPropertyType::from(MetadataType::from(MetadataTypeInner::NamedType(
                        NamedType {
                            ty: "account-id".to_string()
                        }
                    )))
                ),
                (
                    "order".to_string(),
                    ObjectPropertyType::from(MetadataTypeInner::MetadataTypeTagged(
                        MetadataTypeTagged::Option(OptionType {
                            value: Box::new(
                                MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::U32)
                                    .into()
                            )
                        })
                    ))
                ),
                (
                    "start-time".to_string(),
                    ObjectPropertyType::from(MetadataTypeInner::NamedType(NamedType {
                        ty: "utc-timestamp".to_string()
                    }))
                ),
                (
                    "end-time".to_string(),
                    ObjectPropertyType::from(MetadataTypeInner::NamedType(NamedType {
                        ty: "utc-timestamp".to_string()
                    }))
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
        }))
        .into()
    );

    assert_eq!(
        types["apply-credit-kv"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(KeyedStateType {
            properties: KeyedStateProperties {
                key: Box::new(
                    MetadataTypeInner::NamedType(NamedType {
                        ty: "credit-id".to_string()
                    })
                    .into()
                ),
                value: Box::new(
                    MetadataTypeInner::NamedType(NamedType {
                        ty: "apply-credit-event".to_string()
                    })
                    .into()
                ),
            }
        }))
        .into()
    );

    assert_eq!(
        types["usage-rollup"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::List(ListType {
            items: Box::new(
                MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(
                    KeyedStateType {
                        properties: KeyedStateProperties {
                            key: Box::new(MetadataTypeTagged::String.into()),
                            value: Box::new(
                                MetadataTypeInner::NamedType(NamedType {
                                    ty: "decimal".to_string()
                                })
                                .into()
                            ),
                        }
                    }
                ))
                .into()
            )
        }))
        .into()
    );

    assert_eq!(
        types["parent"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Option(OptionType {
            value: Box::new(MetadataTypeTagged::String.into())
        }))
        .into()
    );

    assert_eq!(
        types["my-row"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::ArrowRow(RowType {
            properties: vec![
                (
                    "name".to_string(),
                    ColumnType::Typed(types::CategoryKind::String)
                ),
                (
                    "count".to_string(),
                    ColumnType::Typed(types::CategoryKind::S32)
                ),
            ]
            .into_iter()
            .collect()
        }))
        .into()
    );

    assert_eq!(
        types["my-nested-enum"],
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Enum(EnumType {
          tagging: Default::default(),
          one_of: vec![
                (
                    "empty".to_string(),
                    EnumVariantType {
                        ty: Some(MetadataTypeInner::None(Default::default()).into()),
                        serde: Default::default(),
                    }
                ),
                (
                    "nested".to_string(),
                    EnumVariantType {
                        ty: Some(
                            MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::Object(
                                ObjectType {
                                    properties: vec![
                                        (
                                            "name".to_string(),
                                            ObjectPropertyType::from(MetadataType::from(MetadataTypeTagged::String))
                                        ),
                                        (
                                            "my-list".to_string(),
                                            ObjectPropertyType::from(MetadataTypeInner::MetadataTypeTagged(
                                                MetadataTypeTagged::List(ListType {
                                                    items: Box::new(
                                                        MetadataTypeInner::MetadataTypeTagged(
                                                            MetadataTypeTagged::Object(ObjectType {
                                                                properties: vec![
                                                                    (
                                                                        "name".to_string(),
                                                                        ObjectPropertyType::from(
                                                                          MetadataType::from(MetadataTypeTagged::String)
                                                                        )
                                                                    ),
                                                                    (
                                                                        "count".to_string(),
                                                                        ObjectPropertyType::from(
                                                                            MetadataType::from(MetadataTypeTagged::S32)
                                                                        )
                                                                    ),
                                                                ]
                                                                .into_iter()
                                                                .collect::<BTreeMap<_, _>>()
                                                            })
                                                        ).into()
                                                    )
                                                })
                                            ))
                                        ),
                                    ]
                                    .into_iter()
                                    .collect::<BTreeMap<_, _>>()
                                }
                            ))
                            .into()
                        ),
                        serde: Default::default(),
                    }
                ),
            ].into_iter().collect()
        }))
        .into()
    )
}

#[test]
fn test_validate_state_row() {
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
    states:
      my-state:
        type: keyed-state
        properties:
          key:
            type: string
          value:
            type: arrow-row
            properties:
              one:
                type: s64
              two:
                type: string

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

    let services = config.services().expect("invalid services");
    let (_state_name, state) = services
        .iter()
        .find(|(name, _)| name == &"basic_operation")
        .unwrap()
        .1
        .states
        .iter()
        .find(|(state_name, _)| *state_name == "my-state")
        .unwrap();

    let state = state.valid_data().expect("state should be valid");

    assert!(matches!(state, StateWrapper::Typed { .. }));

    let StateWrapper::Typed(ty) = state else {
        panic!("should match")
    };

    let MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::KeyedState(kv)) =
        &ty.inner_type.ty
    else {
        panic!("should match")
    };

    let value = kv.properties.value.as_ref();
    assert_eq!(
        &value.ty,
        &MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::ArrowRow(RowType {
            properties: vec![
                (
                    "one".to_string(),
                    ColumnType::Typed(types::CategoryKind::S64)
                ),
                (
                    "two".to_string(),
                    ColumnType::Typed(types::CategoryKind::String)
                ),
            ]
            .into_iter()
            .collect()
        }))
    );
}

#[test]
fn test_parse_dev_config() {
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

dev:
  imports:
    - pkg: example/my-package@0.1.0
      path: test/bank-update
  topics:
    sentences:
      name: test-sentences
      consumer:
        default_starting_offset:
          value: 0
          position: End
      schema:
        value:
          type: string
          converter: raw
"
    .to_string();

    let config = parse(&yaml).expect("parse yaml");
    let dev_config = config.dev().expect("Should have dev config");

    assert_eq!(dev_config.imports[0].package.namespace, "example");
    assert_eq!(dev_config.imports[0].package.name, "my-package");
    assert_eq!(dev_config.imports[0].package.version, "0.1.0");
    assert_eq!(
        dev_config.imports[0].path,
        Some(String::from("test/bank-update"))
    );

    let topic = dev_config.topics.get("sentences").expect("topic to exist");

    assert_eq!(topic.name, Some("test-sentences".to_string()));
    assert_eq!(
        topic.consumer.as_ref().unwrap().default_starting_offset,
        Some(OffsetWrapper::End(0))
    );
    assert_eq!(
        topic.schema.value.ty.ty,
        MetadataTypeInner::MetadataTypeTagged(MetadataTypeTagged::String)
    );
    assert_eq!(
        topic.schema.value.converter.clone().unwrap(),
        SerdeConverter::Raw
    );
}

#[test]
fn test_json_schema_def() {
    let schema = schema_for!(DataflowDefinitionConfig);
    let output = serde_json::to_string_pretty(&schema).expect("Failed to serialize JSON");
    assert!(output.contains("$schema"));
}
