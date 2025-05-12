## Stripe Reduce

The dataflow uses [jaq package](../../packages/jaq) to convert Stripe events to smaller relevant events that are easier to work with. 

Checkout the [dataflow.yaml](./dataflow.yaml) for details.

### Run the Dataflow

```bash
sdf run
```

Use `--ui` to see the dataflow UI.

### Test Events

Generate one event:

```bash
fluvio produce stripe-origin-events -f ./sample-data/invoice-created.json --raw
```

Generate all events:

```bash
fluvio produce stripe-origin-events -f ./sample-data/events.json
```

Check the result:

```bash
fluvio consume stripe-events -Bd -O json
```

### Test "Not Handled" Events

These events have not been processed by JAQ. It is left to the user to add any additional events as required by the use case.

```bash
fluvio produce stripe-origin-events -f ./sample-data/events-not-handled.json
```

Check the result:

```bash
fluvio consume stripe-events-not-handled -Bd -O json
```
