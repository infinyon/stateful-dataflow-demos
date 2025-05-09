## JAQ

Currently, the only way to support Jaq is by compiling the filter rules into the code.
The file we've compiled is in the following directory:
* [samples-transforms/stripe-transforms.jq](./sample-transforms/stripe-transforms.jq)

After we add a solution that can read files, we should be able to read this file without having to recompile the code.

### Test

As JAQ is processing binary data, the easiest way to test is by using `Cargo test`:

```bash
cd rust/jaq && cargo test && cd ../../
```

### Code Changes

Checkout the the code in the following files:
* [./rust/Cargo.toml](./rust/jaq/Cargo.toml)
* [./rust/src/jaq-transform.rs](./rust/jaq/src/jaq_transform.rs)