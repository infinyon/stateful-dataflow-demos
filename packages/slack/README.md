### Slack Package

The slack pakage already contains the package file with types generated from [slack-schema.json](./slack-shema.json). To regenerate the types, check the [Generate Types](#generate-types) section. You'll need to manually copy/paste it in the [sdf-pacakge.yaml](./sdf-package.yaml) file.

#### SDF Tests

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/title-event.json
sdf test function test-event --value-file sample-data/fields-event.json
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
cd ../../../sdf-types-generator && \
cargo run -- -s ../stateful-dataflow-demos/packages/slack/slack-shema.json -k /components/schemas/slack_event > ../stateful-dataflow-demos/packages/slack/types.yaml && \
cd ../stateful-dataflow-demos/packages/slack 
```

Checkout the [types.yaml](./types.yaml) file for the generated types.