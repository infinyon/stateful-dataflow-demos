## JAQ

JAQ is a rust implementation of the [jq](https://stedolan.github.io/jq/) command line tool.
You may use the following tool to test you `JQ` expression: - https://gedenkt.at/jaq/

### Prerequisites

The JAQ filtering format expects an if/else expression as to match a `filter-map` SDF operation. This construct allows you to use a filter operation as well as a conditional operation. 

If you want to test localy, you must run:

```bash
sdf build
```

### Filter Statement

If the result always returns a value, then the following syntax should be used:

```bash
sdf test function jaq-transform \
  --with jaq-filter=".[] | .name" \
  --value-file /dev/stdin <<'EOF'
[
  { "name": "Sammy",  "type": "shark",    "clams": 5 },
  { "name": "Bubbles", "type": "orca",     "clams": 3 },
  { "name": "Splash",  "type": "dolphin",  "clams": 2 }
]
EOF
```

Alternatively, read the input from a file:

```bash
sdf test function jaq-transform \
  --with jaq-filter=".[] | .name" \
  --value-file ./sample-data/fish.json
```

### Conditional Statement

In a conditional statement, if the result does not math, it must return a `null` value:

```bash
if 
  <condition>
then 
  <result>
else 
  null
end
```

In this example, we return only players a high score:

```bash
sdf test function jaq-transform \
  --with "jaq-filter=$(cat <<'FILTER'
.[]
| if has("high_score") 
  then .
  else null
  end
FILTER
)" \
  --value-file /dev/stdin <<'EOF'
[
  { "player_id": 1, "username": "ShadowNinja",  "high_score": 1200},
  { "player_id": 2, "username": "PixelWizard"},
  { "player_id": 3, "username": "QuestMaster",  "high_score": 4500},
  { "player_id": 4, "username": "SpeedRacer",   "high_score": 3000},
  { "player_id": 5, "username": "SkyPirate"}
]
EOF
```

Read the input from a file:

```bash
sdf test function jaq-transform \
  --with 'jaq-filter=.[] | if has("high_score") then . else null end' \
  --value-file ./sample-data/players.json
```


### Cargo Test

To run Cargo tests perform the following command:

```bash
cd rust/jaq && cargo test && cd ../../
```

### Code Changes

The package code changes are located at:

* [./rust/Cargo.toml](./rust/jaq/Cargo.toml)
* [./rust/src/jaq-transform.rs](./rust/jaq/src/jaq_transform.rs)