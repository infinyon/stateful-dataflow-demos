### Slack Package

The slack pakage already contains the package file with types generated from [slack-schema.json](./slack-shema.json). To regenerate the types, check the [Generate Types](#generate-types) section. You'll need to manually copy/paste it in the [sdf-pacakge.yaml](./sdf-package.yaml) file.

The following sections shows you how to test the package using [sdf](#sdf-tests) or [cargo](#cargo-tests).

#### SDF Tests

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/events/title-event.json
sdf test function test-event --value-file sample-data/events/fields-event.json
```

Test field to event function:

```bash
sdf test function make-slack-fields-event --value-file sample-data/objects/title-obj.json
sdf test function make-slack-fields-event --value-file sample-data/objects/fields-obj.json
```

### Cargo Tests

To run cargo tests, use the following commands:

```bash
cd rust/slack-package
cargo add --dev serde_json 
cargo test
```

### Slack Tests

```bash
curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' https://hooks.slack.com/services/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
```

```bash
curl -X POST -H 'Content-type: application/json' --data '{"blocks":[{"text":{"text":"New *Stripe* event - *InvoiceEventType::InvoiceCreated* (Some(InvoiceStatus::Draft)) :memo:","type":"mrkdwn"},"type":"section"},{"fields":[{"text":"*Account:*\nInfinyOn (US)","type":"mrkdwn"},{"text":"*Customer:*\nNick Cardin <nick+fc9@infinyon.com>","type":"mrkdwn"},{"text":"*Amount Due:*\n0.00 USD","type":"mrkdwn"},{"text":"*Amount Paid:*\n0.00 USD","type":"mrkdwn"},{"text":"*Period:*\nApr 03, 2025 - Apr 03, 2025","type":"mrkdwn"},{"text":"*Items:*\n-","type":"mrkdwn"}],"type":"section"}]}' https://hooks.slack.com/services/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
```

### Generate Types

To generate the types, download [sdf-types-generator](http://github.com/infinyon/sdf-types-generator) and perform the following commands:

```bash
cd ../../../sdf-types-generator 
cargo run -- -s ../stateful-dataflow-demos/packages/slack/slack-shema.json -k /components/schemas/slack_event -k /components/schemas/slack_obj
cd ../stateful-dataflow-demos/packages/slack
```