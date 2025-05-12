## Stateful Dataflow Demos

Dataflows for complex use cases:

* [Stripe to Slack](./stripe-to-slack)


### DataFlow Architecture

Implementing an event-driven dataflow can be challenging — raw webhooks from third-party vendors like Stripe or GitHub are typically verbose and unwieldy for analytics. To simplify downstream processing, we split the pipeline into two distinct stages:

##### Reduce
Converts raw vendor events into compact, Fluvio-native types.
* Example: [stripe-reduce](./stripe-to-slack//stripe-reduce/)

##### Process
Consumes those reduced types and executes one or more domain-specific transformations or actions.
* Example: [stripe-slack](./stripe-to-slack//stripe-slack/)

You can chain as many Process pipelines as your use cases require — each one targets a specific analytic, notification, or enrichment task.
