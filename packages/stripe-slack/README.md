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
sdf test function stripe-to-slack --value-file sample-data/customer-created.json
sdf test function stripe-to-slack --value-file sample-data/customer-subscription-created.json
sdf test function stripe-to-slack --value-file sample-data/customer-subscription-updated.json
sdf test function stripe-to-slack --value-file sample-data/customer-updated.json
sdf test function stripe-to-slack --value-file sample-data/invoice-created.json
sdf test function stripe-to-slack --value-file sample-data/invoice-finalized.json
sdf test function stripe-to-slack --value-file sample-data/invoice-paid.json
sdf test function stripe-to-slack --value-file sample-data/invoice-payment-failed.json
sdf test function stripe-to-slack --value-file sample-data/invoice-payment-succeeded.json
sdf test function stripe-to-slack --value-file sample-data/invoice-sent.json
sdf test function stripe-to-slack --value-file sample-data/invoice-updated.json
sdf test function stripe-to-slack --value-file sample-data/invoiceitem-created.json
sdf test function stripe-to-slack --value-file sample-data/issuing-authorization-created.json
sdf test function stripe-to-slack --value-file sample-data/issuing-card-created.json
sdf test function stripe-to-slack --value-file sample-data/issuing-card-updated.json
sdf test function stripe-to-slack --value-file sample-data/issuing-cardholder-created.json
sdf test function stripe-to-slack --value-file sample-data/issuing-cardholder-updated.json
sdf test function stripe-to-slack --value-file sample-data/issuing-dispute-created.json
sdf test function stripe-to-slack --value-file sample-data/issuing-dispute-submitted.json
sdf test function stripe-to-slack --value-file sample-data/payment-intent-created.json
sdf test function stripe-to-slack --value-file sample-data/payout-created.json
sdf test function stripe-to-slack --value-file sample-data/payout-paid.json
sdf test function stripe-to-slack --value-file sample-data/payout-reconciliation-completed.json
sdf test function stripe-to-slack --value-file sample-data/payout-update.json
sdf test function stripe-to-slack --value-file sample-data/source-chargeable.json
sdf test function stripe-to-slack --value-file sample-data/subscription-schedule-created.json
sdf test function stripe-to-slack --value-file sample-data/subscription-schedule-updated.json
sdf test function stripe-to-slack --value-file sample-data/topup-created.json
sdf test function stripe-to-slack --value-file sample-data/topup-succeeded.json
```
