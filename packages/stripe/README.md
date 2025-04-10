### Stripe to Slack

This package has already generated the types. The following commands are for testing. 

Check the bottom of this file for the types re-generation.

#### Test Commands

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/invoice-created.json
```

#### Generate SDF types

```bash
cargo run -- -s stripe-schema.json -k /components/schemas/stripe_event
```
