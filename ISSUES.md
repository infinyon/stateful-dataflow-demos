# Issues

* [#1. Keywords with numbers are parsed with a `_`.](#1-keywords-with-numbers-are-parsed-with-a-_)
* [#2. I should be able to write `raw` into a `typed` topic](#2-i-should-be-able-to-write-raw-into-a-typed-topic)
* [#3. Suppoort for hierarchhical imports in packages](#3-support-for-hierarchical-imports-in-packages)
* [#4. Add `init` support for packages](#4-add-init-support-for-packages)
* [#5. Support for `raw` in `sdf build`](#5-support-for-raw-in-sdf-build)


### 1. Keywords with numbers are parsed with a `_`.

```yaml
      line1:
        type: string
        optional: true
```

The following code:
```rust
pub(crate) fn bytes_to_event(raw: Bytes) -> Result<StripeEvent> {
    let event: StripeEvent = serde_json::from_slice(&raw)
        .map_err(|e| sdfg::anyhow::anyhow!("failed to parse StripeEvent JSON: {}", e))?;
    Ok(event)
}
```

Generates an error:
```bash
error[E0609]: no field `line_1` on type `&bindings::examples::stripe::types::Address`
  --> src/lib.rs:86:9
   |
86 |         line_1: Option<String>,
   |         ^^^^^^ unknown field
   |
help: a field with a similar name exists
   |
86 |         line1: Option<String>,
   |         ~~~~~
```

### 2. I should be able to write `raw` into a `typed` topic

JAQ is converting the `raw` into `raw` so it stays generic.
But our topic does not support writing `raw` and reading `typed`.

Topic definitions:

```yaml
  stripe-origin-events:
    schema:
      value:
        type: bytes
      converter: raw

  stripe-events:
    schema:
      value:
        type: stripe-event
```

Service that uses JAQ to transform the `raw` into a `raw` topic:

```yaml
  origin-to-stripe:
    sources:
      - type: topic
        id: stripe-origin-events

    transforms:
      - operator: map
        uses: jaq-transform

    sinks:
      - type: topic
        id: stripe-events
```

The function prototype is as follows:

```rust
#[sdf(fn_name = "jaq-transform")]
pub(crate) fn jaq_transform(input: Bytes) -> Result<Bytes> {
    run_jaq_transform(input, JAQ_FILTER)
```

I get the following error:

```rust
validation error: Dataflow Config failed validation

    Service `origin-to-stripe` is invalid:
        Sink `stripe-events` is invalid:
            Transforms block is invalid:
                service output type `bytes` does not match sink input type `stripe-event`
```

Can we allow overwride in the `sink`?

```yaml
    sinks:
      - type: topic
        id: stripe-events
        schema:
          value:
            type: bytes
          converter: raw
```

### 3. Support for hierarchical imports in packages

Packages that import other packages don't work in the dataflow. `package not in the hub` error.

```yaml
apiVersion: 0.6.0

meta:
  name: stripe-slack
  version: 0.0.1
  namespace: examples

imports:
  - pkg: examples/stripe-types@0.0.1
    types:
      - name: stripe-event
  - pkg: examples/slack-package@0.0.1
    types:
      - name: slack-event

functions:
  stripe-to-slack:
    operator: map
    inputs:
      - name: se
        type: stripe-event
    output:
      type: slack-event

dev:
  imports:
    - pkg: examples/stripe-types@0.0.1
      path: ../stripe
    - pkg: examples/slack-package@0.0.1
      path: ../slack
```

I need to use --dev for `sdf build` to work, which creates issues for `sdf run`.

#### Solution

Default to dev mode for `sdf build` and `sdf run`. Use `--hub` if you want to call-up packages from the hub.

### 4. Add `init` support for packages

Right now, I add jaq transforms in a `rust constant`.
We nee a way to import `jaq tranform filter` from a file. Adding it inside the SDF is going to create additional issues. 

The file is big - [jaq-transform.rs](./packages/jaq/sample-transforms/stripe-transforms.jq).

### 5. Add support for `--input` `--output` to `sdf test`

* Issue [https://github.com/infinyon/sdf/issues/2475](https://github.com/infinyon/sdf/issues/2475)