## Stripe Clean

Use JAQ to convert Stripe events to smaller relevant structures.

```bash
sdf run
```

## Generate Records & Check Result

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