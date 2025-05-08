## Stripe to Slack Dataflow

The dataflow uses [stripe-types](../../packages/stripe-types), [slack-types](../../packages/slack-types), and [stripe-to-slack](../../packages/stripe-to-slack) packages to convert Stripe events to Slack notifications.

Checkout the [dataflow.yaml](./dataflow.yaml) for details.

### Run the Dataflow

```bash
sdf run
```

Use `--ui` to see the dataflow UI.


### Generate Records & Check Result

Generate a test event:

```bash
fluvio produce stripe-events -f ./sample-data/invoice-created.json --raw
```

Check the result:

```bash
fluvio consume slack-stripe-events -B -O json
```