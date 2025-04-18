### Stripe to Slack Dataflow

The end-to-end use-case will be a webhook that listens to Stripe events, converts them to SDF types, and sends notification to Slack.


#### 1. Create a webhook for Stripe 

On InfinyOn cloud use the [stripe-webhook.toml](./stripe-webhook.yaml) to create a webhook:

  ```bash
  fluvio cloud webhook create -c use-cases/stripe/stripe-webhook.yaml
  ```

Grab the webhook URL from the output and add it to Stripe as described in the [next section](#2-add-webhook-to-stripe).


#### 2. Add Webhook to Stripe

1. Open the Stripe webhook section - https://dashboard.stripe.com/test/webhooks
  - Enable derised events
  - Copy/Paste the webhook URL


2. Generate some events and see them in the `stripe-origin-events` topic:

  ```bash 
  fluvio consume stripe-origin-events -Bd -O json
  ```

#### 3. Create a Slack Connector

Create a Slack connector that notifies when a Stripe event is received.

1. Add Slack App - https://api.slack.com/apps
2. Enable webhooks - https://hooks.slack.com/services/
3. Add webhook secret to InfinyOn Cloud

  ```bash
  fluvio cloud secret set SLACK_BILLING_SECRET
  ```

4. Start the Slack Billing Connector - [slack-billing-connector.yaml](./use-cases/stripe/slack-billing-connector.yaml)

  ```bash
  fluvio cloud connector create -c use-cases/stripe/slack-billing-connector.yaml
  ```

#### 3. Start the Dataflows

Start the `stripe-clean` dataflow that converts Stripe events to smaller relevant structures - [stripe-clean.yaml](../stripe-clean/dataflow.yaml)

```bash
cd ../stripe-clean
sdf run
```

Start the dataflow that listens for events from Stripe and converts them for Slack - [stripe-dataflow.yaml](dataflow.yaml)

```
sdf run
```

#### 5. Test

Generate a test event from local file & show it display in Slack.

```bash
fluvio produce stripe-origin-events -f packages/stripe/sample-data/invoice-created.json
 --raw
```

Then use Stripe UI to trigger additional events.
