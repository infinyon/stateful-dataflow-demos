## Stripe Reduce

The dataflow uses [jaq package](../../packages/jaq) to convert Stripe events to smaller relevant events that are easier to work with. 

Checkout the [dataflow.yaml](./dataflow.yaml) for details.

### Run the Dataflow

```bash
sdf run
```

Use `--ui` to see the dataflow UI.

### Generate Records & Check Result

Generate one event:

```bash
fluvio produce stripe-origin-events -f ./sample-data/event-send-invoice.json --raw
```

Generate all events:

```bash
fluvio produce stripe-origin-events -f ./sample-data/events.json
```

Check the result:

```bash
fluvio consume stripe-events -Bd
```