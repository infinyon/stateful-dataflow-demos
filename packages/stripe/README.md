### Stripe to Slack

This package has already generated the types. The following commands are for testing. 

Check the bottom of this file for the types re-generation.

#### Test Commands

Test events parsing:

```bash
sdf test function bytes-to-event --value-file sample-data/invoice-created.json
```

### Generate Types

To generate the types, download [sdf-types-generator](http://github.com/infinyon/sdf-types-generator) and perform the following commands:

```bash
cd ../../../sdf-types-generator && cargo run -- -s ../stateful-dataflow-demos/packages/stripe/stripe-schema.json -k /components/schemas/stripe_event > ../stateful-dataflow-demos/packages/stripe/types.yaml && cd ../stateful-dataflow-demos/packages/stripe
```

Checout the [types.yaml](types.yaml) file.

#### Generate SDF types

```bash
cargo run -- -s stripe-schema.json -k /components/schemas/stripe_event
```
