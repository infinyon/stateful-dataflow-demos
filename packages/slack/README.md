### Slack Package

This package has already generated the types. The following commands are for testing. 

Check the bottom of this file for the types re-generation.

#### Test Commands

Test events parsing:

```bash
sdf test function test-event --value-file sample-data/events/title-event.json
sdf test function test-event --value-file sample-data/events/fields-event.json
```

Test field to event function:

```bash
sdf test function make-slack-fields-event --value-file sample-data/objects/title-obj.json
sdf test function make-slack-fields-event --value-file sample-data/objects/fields-obj.json
```

#### Types Generator

To generate the types again, run:

```bash
cargo run -- -s slack-shema.json -k /components/schemas/slack_event
```