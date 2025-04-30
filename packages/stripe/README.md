### Stripe Package

Ensures that stripe events have been converted in the correct format as defined in the [stripe-schema.json](./stripe-schema.json).
The conversion was perfomed using [jaq](../jaq/REAME.MD).

#### Test Commands

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/charge-succeeded.json
sdf test function test-event --value-file sample-data/customer-created.json
sdf test function test-event --value-file sample-data/customer-updated.json
sdf test function test-event --value-file sample-data/customer-subscription-created.json
sdf test function test-event --value-file sample-data/customer-subscription-updated.json
sdf test function test-event --value-file sample-data/invoice-created.json
sdf test function test-event --value-file sample-data/invoice-finalized.json
sdf test function test-event --value-file sample-data/invoice-paid.json
sdf test function test-event --value-file sample-data/invoice-payment-failed.json
sdf test function test-event --value-file sample-data/invoice-payment-succeeded.json
sdf test function test-event --value-file sample-data/invoice-sent.json
sdf test function test-event --value-file sample-data/invoice-updated.json
sdf test function test-event --value-file sample-data/invoiceitem-created.json
sdf test function test-event --value-file sample-data/issuing-authorization-created.json
sdf test function test-event --value-file sample-data/issuing-card-created.json
sdf test function test-event --value-file sample-data/issuing-card-updated.json
sdf test function test-event --value-file sample-data/issuing-cardholder-created.json
sdf test function test-event --value-file sample-data/issuing-cardholder-updated.json
sdf test function test-event --value-file sample-data/issuing-dispute-created.json
sdf test function test-event --value-file sample-data/issuing-dispute-submitted.json
sdf test function test-event --value-file sample-data/payment-intent-created.json
sdf test function test-event --value-file sample-data/payout-created.json
sdf test function test-event --value-file sample-data/payout-paid.json
sdf test function test-event --value-file sample-data/payout-reconciliation-completed.json
sdf test function test-event --value-file sample-data/payout-update.json
sdf test function test-event --value-file sample-data/source-chargeable.json
sdf test function test-event --value-file sample-data/subscription-schedule-created.json
sdf test function test-event --value-file sample-data/subscription-schedule-updated.json
sdf test function test-event --value-file sample-data/topup-created.json
sdf test function test-event --value-file sample-data/topup-succeeded.json
```

### Generate Types

To generate the types, download [sdf-types-generator](http://github.com/infinyon/sdf-types-generator) and perform the following commands:

```bash
cd ../../../sdf-types-generator && \
cargo run -- -s ../stateful-dataflow-demos/packages/stripe/stripe-schema.json -k /components/schemas/stripe_event > ../stateful-dataflow-demos/packages/stripe/types.yaml && \
cd ../stateful-dataflow-demos/packages/stripe
```

Troubleshooting command:

```bash
cd ../../../sdf-types-generator && \
cargo run -- -s ../stateful-dataflow-demos/packages/stripe/stripe-schema.json -k /components/schemas/stripe_event -d > ../stateful-dataflow-demos/packages/stripe/dump-spec.json && \
cd ../stateful-dataflow-demos/packages/stripe
```

Checout the [types.yaml](types.yaml) file.
