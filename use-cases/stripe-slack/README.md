### Stripe to Slack Dataflow

The end-to-end use-case will be a webhook that listens to Stripe events, converts them to SDF types, and sends notification to Slack.


#### 1. Start a Webhook

Start a webhook that Stripe will call - [stripe-webhook.toml](./use-cases/stripe/stripe-webhook.yaml).

  ```bash
  fluvio cloud webhook create -c use-cases/stripe/stripe-webhook.yaml
  ```

**Note:** The webhook has the `jaq` transformations that convert the Stripe types into SDF types.


#### 2. Create a Slack Connector

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

#### 3. Start the Dataflow

Start the dataflow that listens for events from Stripe and converts them for Slack - [stripe-dataflow.yaml](./use-cases/stripe/stripe-dataflow.yaml)

```
sdf run
```

#### 5. Test

Generate a test event & show it display in Slack.

```bash
fluvio produce stripe-e
vents -f packages/stripe/sample-data/invoice-created.json
 --raw
```

Then use Stripe UI to trigger additional events.

