### Stripe to Slack Package

This package imports the event types from Stripe and Slack pacckages, and handles the translation between the two.

The code is already part of the package. Checkout the following files:
* [./rust/stripe-slack/src/stripe_to_slack.rs](./rust/stripe-slack/src/stripe_to_slack.rs)
* [./rust/stripe-slack/Cargo.toml](./rust/stripe-slack/Cargo.toml)

Checkout the Rust types in the [./rust/stripe-slack/helpers/types.rs](./rust/stripe-slack/helpers/types.rs)
The types were generated with the following command:

```bash
sdf package types
```

### Test with Cargo

To test using Rust and Cargo run the following commands:

```bash
cd ./rust/stripe-slack && cargo test && cd ../..
```

### Test with SDF

To test using SDF run the following commands:

```bash
sdf test function stripe-to-slack --value-file sample-data/charge-succeeded.json
```
