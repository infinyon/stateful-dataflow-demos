### Slack Package

The slack package ensures the Slack events are structured based on the [slack-schema.json](./slack-shema.json) schema types.

#### SDF Tests

Build the project:

```bash
sdf build
```

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/title-event.json
sdf test function test-event --value-file sample-data/fields-event.json
```

### Generate Types

To generate the types, download [sdf-types-generator](http://github.com/infinyon/sdf-types-generator) and perform the following commands:

```bash
cd ../../../sdf-types-generator && \
cargo run -- -s ../stateful-dataflow-demos/packages/slack-types/slack-shema.json -k /components/schemas/slack_event > ../stateful-dataflow-demos/packages/slack-types/types.yaml && \
cd ../stateful-dataflow-demos/packages/slack-types 
```

Checkout the [types.yaml](./types.yaml) file for the generated types.
