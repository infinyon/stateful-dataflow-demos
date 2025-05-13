### Stripe to Slack Dataflow

The end-to-end use-case will be a webhook that listens to Stripe events, converts them to SDF types, and sends notification to Slack. 

### Prerequisites

This dataflow reuquires an incoming webhook, which requires an account on [InfinyOn Cloud](https://infinyon.cloud).
If you want to run on test data locally, skip to [Deploy and on Local Machine (Test)](#deploy-on-local-machine-test).

<hr />

### Deploy on InfinyOn Cloud (Production)

The following steps will deploy the following resources:

1. [Stripe webhook](./stripe-webhook.yaml) for incoming events.
2. [Slack connector](./slack-connector.yaml) for outgoing notifications.
3. [Stripe reduce dataflow](../stripe-reduce) for converting Stripe types to known types.
4. [Stripe to Slack dataflow](./stripe-slack) for converting Stripe events to Slack notifications.

All packages have been published on the Hub, and they are ready for use in dataflows.

#### 1. Create a webhook for Stripe 

On InfinyOn cloud use the [stripe-webhook.toml](./stripe-webhook.yaml) to create a webhook:

  ```bash
  fluvio cloud webhook create -c stripe-webhook.yaml
  ```

Grab the webhook URL from the output and add it to Stripe as described in the [next section](#2-add-webhook-to-stripe).


#### 2. Add Webhook to Stripe

1. Open the Stripe webhook section - https://dashboard.stripe.com/test/webhooks
  - Enable the events you are interested in.
  - Copy/Paste the webhook URL


2. Generate some events and see them in the `stripe-origin-events` topic:

  ```bash 
  fluvio consume stripe-origin-events -Bd -O json
  ```

#### 3. Create a Slack Connector

Create a Slack connector that notifies when a Stripe event is received.

1. Add Slack App - https://api.slack.com/apps
2. Add an `Incoming Webhook` to get the webhook URL and secret:
  - https://hooks.slack.com/services/XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZZZZZZZZZ
3. Take the secret components and add to InfinyOn Cloud:

  ```bash
  fluvio cloud secret set SLACK_BILLING_SECRET
  ```

4. Start the Slack Billing Connector - [slack-connector.yaml](slack-connector.yaml)

  ```bash
  fluvio cloud connector create -c slack-connector.yaml
  ```

#### 3. Deploy Stripe Reduce Dataflow

Deploy the `stripe-reduce` dataflow that converts Stripe events to events defined in InfinyOn Stripe Schema - [stripe-reduce/dataflow.yaml](./stripe-reduce/dataflow.yaml)

```bash
cd ./stripe-reduce
sdf deploy
```

This dataflow uses [jaq package](../packages/jaq) to convert Stripe events into a simpler form that is easier to work with.


#### 4. Deploy Stripe to Slack Dataflow

Deploy the `stripe-slack` dataflow that converts Stripe events to Slack notifications - [stripe-slack/dataflow.yaml](./stripe-slack/dataflow.yaml)

```bash
cd ./stripe-slack
sdf deploy
```

This dataflow uses [stripe-types](../packages/stripe-types), [slack-types](../packages/slack-types), and [stripe-to-slack](../packages/stripe-to-slack) packages to convert Stripe events to Slack notifications.


#### 5. End-to-end Test

Then use Stripe UI to trigger events and watch the notifications in Slack.


<hr />

### Deploy on Local Machine (Test)

You may run and test the dataflows on your local machine by producing and consuming events to and from topics.

#### 1. Build the Packages

First, build the packages that are used by the dataflows.

1. Build `jaq` package

```bash
cd ../packages/jaq && sdf build && cd ../../stripe-to-slack
```

2. Build `slack-types` package

```bash
cd ../packages/slack-types && sdf build && cd ../../stripe-to-slack
```

3. Build `stripe-types` package

```bash
cd ../packages/stripe-types && sdf build && cd ../../stripe-to-slack
```

4. Build `stripe-slack` package

```bash
cd ../packages/stripe-slack && sdf build && cd  ../../stripe-to-slack
```

#### 2. Start Stripe Reduce Dataflow

Start the `stripe-reduce` dataflow that converts Stripe events to events defined in InfinyOn Stripe Schema - [stripe-reduce/dataflow.yaml](./stripe-reduce/dataflow.yaml)

```bash
cd ./stripe-reduce
sdf run
```

This dataflow uses [jaq package](../packages/jaq) to convert Stripe events into a simpler form that is easier to work with.


#### 3. Start Stripe to Slack Dataflow

In another terminal, start the `stripe-slack` dataflow. This dataflow converts Stripe events to Slack notifications - [stripe-slack/dataflow.yaml](./stripe-slack/dataflow.yaml)

```bash
cd ./stripe-slack
sdf run
```

This dataflow uses [stripe-types](../packages/stripe-types), [slack-types](../packages/slack-types), and [stripe-to-slack](../packages/stripe-to-slack) packages to convert Stripe events to Slack notifications.


#### 4. End-to-end Test

Generate a test event from local file & show it display in Slack.

```bash
fluvio produce stripe-origin-events -f packages/stripe-to-slack/sample-data/event-send-invoice.json --raw
```

Checkout the Slack topic to see the notification.

```bash
fluvio consume slack-stripe-events -Bd -O json
```

Slack should also display the notifications.
