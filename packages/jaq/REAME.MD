
## Test

```bash
sdf test function jaq-transform --value-file sample-data/event1.json
```

## Add dependencies

```bash
jaq-core = "2.0.0"
jaq-std = "2.0.0"
jaq-json = { version = "1.0.0", features = ["serde_json"] }
serde_json = {version = "1.0.81", features = ["preserve_order"]}
```