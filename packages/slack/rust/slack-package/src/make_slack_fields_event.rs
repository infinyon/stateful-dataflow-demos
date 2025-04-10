use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::slack_package_types::types::SlackEvent;
use crate::bindings::examples::slack_package_types::types::FieldsObj;
#[allow(unused_imports)]
use crate::bindings::examples::slack_package_types::types::*;

#[sdf(fn_name = "make-slack-fields-event")]
pub(crate) fn make_slack_fields_event(obj: FieldsObj) -> Result<SlackEvent> {
    let mut blocks = vec![];

    // Add Title
    let title = SlackEventUntagged::TextSection(
        TextSection {
            text: TextObject {
                text: obj.title.to_string(),
                type_: TextObjectType::Mrkdwn,
            },
            type_: SectionTextType::Section,
        }
    );
    blocks.push(title);

    // Collect Fields
    let mut fields_list = vec![];
    for field in obj.fields {
        fields_list.push(TextObject {
            text: field.to_string(),
            type_: TextObjectType::Mrkdwn,
        });
    };
    let fields = SlackEventUntagged::FieldsSection(
        FieldsSection {
            fields: fields_list,
            type_: SectionFieldsType::Section,
        }
    );
    blocks.push(fields);

    // Build and return SlackEvent
    Ok(SlackEvent {blocks})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_slack_fields_event() {
        let obj = FieldsObj {
            title: "Title".to_string(),
            fields: vec![
                "Field1".to_string(),
                "Field2".to_string(),
            ],
        };
        let result = "{\"blocks\":[{\"TextSection\":{\"text\":{\"text\":\"Title\",\"type_\":\"Mrkdwn\"},\"type_\":\"Section\"}},{\"FieldsSection\":{\"fields\":[{\"text\":\"Field1\",\"type_\":\"Mrkdwn\"},{\"text\":\"Field2\",\"type_\":\"Mrkdwn\"}],\"type_\":\"Section\"}}]}";

        let slack_event =  make_slack_fields_event(obj).unwrap();
        let json_event = serde_json::to_string(&slack_event).unwrap();

        assert_eq!(json_event, result);
    }
}