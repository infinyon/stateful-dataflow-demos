use sdfg::Result;
use sdfg::sdf;
use crate::bindings::examples::stripe_slack_types::types::SlackEvent;
use crate::bindings::examples::stripe_slack_types::types::StripeEvent;
#[allow(unused_imports)]
use crate::bindings::examples::stripe_slack_types::types::*;

use chrono::{Utc, TimeZone};

// ----- Main function -----

#[sdf(fn_name = "stripe-to-slack")]
pub(crate) fn stripe_to_slack(se: StripeEvent) -> Result<SlackEvent> {
    match se.data {
        EventData::Invoice(ref iv)              => Ok(invoice_to_slack_event(iv, se.livemode)),
        EventData::Invoiceitem(ref ii)          => Ok(invoiceitem_to_slack_event(ii, se.livemode)),
        EventData::Charge(ref ch)               => Ok(charge_to_slack_event(ch, se.livemode)),
        EventData::Customer(ref cu)             => Ok(customer_to_slack_event(cu, se.livemode)),
        EventData::IssuingAuthorization(ref ia) => Ok(issuingauthorization_to_slack_event(ia, se.livemode)),
        EventData::IssuingCard(ref ic)          => Ok(issuingcard_to_slack_event(ic, se.livemode)),
        EventData::IssuingCardholder(ref ih)    => Ok(issuingcardholder_to_slack_event(ih, se.livemode)),
        EventData::IssuingDispute(ref idp)      => Ok(issuingdispute_to_slack_event(idp, se.livemode)),
        EventData::PaymentIntent(ref pi)        => Ok(paymentintent_to_slack_event(pi, se.livemode)),
        EventData::Payout(ref po)               => Ok(payout_to_slack_event(po, se.livemode)),
        EventData::Source(ref so)               => Ok(source_to_slack_event(so, se.livemode)),
        EventData::SubscriptionSchedule(ref ss) => Ok(subscriptionschedule_to_slack_event(ss, se.livemode)),
        EventData::Topup(ref tu)                => Ok(topup_to_slack_event(tu, se.livemode))      
    }
}

// ----- Invoice handling -----

fn invoice_to_slack_event(inv: &Invoice, livemode: bool) -> SlackEvent {
    let status_text = inv.status.as_ref().map(|s| format!("{:?}", s)).unwrap_or_default();
    let event_clean = human_event_type(&inv.event_type);

    let live_mode = if !livemode { " :memo:" } else { ":white_check_mark:" };
    let title_text = format!("New *Stripe* event – *{}* ({}){}", event_clean, status_text, live_mode);

    // Make title
    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { 
            type_: TextObjectType::Mrkdwn, 
            text: title_text.clone() 
        },
    });

    // Collect fields
    let mut raw = Vec::new();
    raw.push(format!("*Account:* {} ({})",
        inv.account_name.clone().unwrap_or_default(),
        inv.account_country.clone().unwrap_or_default()
    ));
    raw.push(format!("*Customer:* {} <{}>",
        inv.customer_name.clone().unwrap_or_default(),
        inv.customer_email.clone().unwrap_or_default()
    ));
    raw.push(format!("*Amount Due:* {:.2} {}", inv.amount_due as f64 / 100.0, inv.currency));
    raw.push(format!("*Amount Paid:* {:.2} {}", inv.amount_paid as f64 / 100.0, inv.currency));
    raw.push(format!("*Period:* {} – {}",
        format_timestamp(inv.period_start),
        format_timestamp(inv.period_end)
    ));
    raw.push(format!("*Items:*\n{}", format_invoice_items(&inv.lines)));

    let fields_objs: FieldsSectionFields = raw.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();

    let fields_block = SlackEventUntagged::FieldsSection(FieldsSection {
        type_: FieldsSectionType::Section,
        fields: fields_objs,
    });

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Invoiceitem handling -----

fn invoiceitem_to_slack_event(ii: &Invoiceitem, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&ii.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* invoice item – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Item ID:* {}", ii.id));
    fields.push(format!("*Amount:* {:.2} {}", ii.amount as f64 / 100.0, ii.currency));
    if let Some(desc) = &ii.description { fields.push(format!("*Description:* {}", desc)); }
    fields.push(format!("*Quantity:* {}", ii.quantity));
    fields.push(format!("*Date:* {}", format_timestamp(ii.date)));

    let field_objs: FieldsSectionFields = fields.into_iter().map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t }).collect();
    let fields_block = SlackEventUntagged::FieldsSection(FieldsSection { type_: FieldsSectionType::Section, fields: field_objs });

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Charge handling -----

fn charge_to_slack_event(ch: &Charge, livemode: bool) -> SlackEvent {
    // Prepare title components
    let status_text = format!("{:?}", ch.status);
    let event_clean = human_event_type(&ch.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title_text = format!(
        "New *Stripe* charge – *{}* ({}){}",
        event_clean,
        status_text,
        memo
    );

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title_text },
    });

    // Build fields
    let mut raw_fields = Vec::new();
    raw_fields.push(format!("*Charge ID:* {}", ch.id));
    raw_fields.push(format!("*Amount:* {:.2} {}", ch.amount as f64 / 100.0, ch.currency));
    raw_fields.push(format!("*Status:* {}", status_text));
    raw_fields.push(format!("*Description:* {}", ch.description.clone().unwrap_or_default()));
    if let Some(cust) = &ch.customer {
        raw_fields.push(format!("*Customer:* {}", cust));
    }

    let fields_objs: FieldsSectionFields = raw_fields
        .into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(FieldsSection {
        type_: FieldsSectionType::Section,
        fields: fields_objs,
    });

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Customer handling -----

fn customer_to_slack_event(c: &Customer, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&c.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* customer – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Customer ID:* {}", c.id));
    if let Some(name) = &c.name { fields.push(format!("*Name:* {}", name)); }
    if let Some(email) = &c.email { fields.push(format!("*Email:* {}", email)); }
    if let Some(desc) = &c.description { fields.push(format!("*Description:* {}", desc)); }

    let field_objs: FieldsSectionFields = fields.into_iter().map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t }).collect();
    let fields_block = SlackEventUntagged::FieldsSection(FieldsSection { type_: FieldsSectionType::Section, fields: field_objs });

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Issuing Authorization handling -----

fn issuingauthorization_to_slack_event(ia: &IssuingAuthorization, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&ia.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* issuing authorization – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Authorization ID:* {}", ia.id));
    fields.push(format!("*Amount:* {:.2} {}", ia.amount as f64 / 100.0, ia.currency));
    fields.push(format!("*Merchant Amount:* {:.2} {}", ia.merchant_amount as f64 / 100.0, ia.merchant_currency));
    fields.push(format!("*Status:* {}", human_event_type(&ia.status)));
    fields.push(format!("*Card:* {}", ia.card));
    fields.push(format!("*Approved:* {}", ia.approved));

    let field_objs: FieldsSectionFields = fields.into_iter().map(|t| 
        TextObject { type_: TextObjectType::Mrkdwn, text: t}
    ).collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Issuing Card handling -----

fn issuingcard_to_slack_event(ic: &IssuingCard, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&ic.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* issuing card – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Card ID:* {}", ic.id));
    fields.push(format!("*Brand:* {}", ic.brand));
    fields.push(format!("*Last4:* {}", ic.last4));
    fields.push(format!("*Status:* {}", human_event_type(&ic.status)));
    fields.push(format!("*Type:* {}",human_event_type(&ic.type_)));
    fields.push(format!("*Exp:* {}/{}", ic.exp_month, ic.exp_year));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Issuing Cardholder handling -----

fn issuingcardholder_to_slack_event(ih: &IssuingCardholder, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&ih.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* cardholder – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Cardholder ID:* {}", ih.id));
    fields.push(format!("*Name:* {}", ih.name));
    fields.push(format!("*Email:* {}", ih.email.clone().unwrap_or_default()));
    fields.push(format!("*Status:* {}", human_event_type(&ih.status)));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Issuing Dispute handling -----

fn issuingdispute_to_slack_event(idp: &IssuingDispute, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&idp.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* issuing dispute – *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Dispute ID:* {}", idp.id));
    fields.push(format!("*Amount:* {:.2} {}", idp.amount as f64 / 100.0, idp.currency));
    fields.push(format!("*Reason:* {}",  human_event_type(&idp.reason)));
    fields.push(format!("*Status:* {}", human_event_type(&idp.status)));
    if let Some(lr) = &idp.loss_reason {
        fields.push(format!("*Loss Reason:* {:?}", lr));
    }

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Payment Intent handling -----

fn paymentintent_to_slack_event(pi: &PaymentIntent, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&pi.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* payment intent - *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Intent ID:* {}", pi.id));
    fields.push(format!("*Amount:* {:.2} {}", pi.amount as f64 / 100.0, pi.currency));
    fields.push(format!("*Status:* {}", human_event_type(&pi.status)));
    if let Some(received) = pi.amount_received { 
        fields.push(format!("*Received:* {:.2} {}", received as f64 / 100.0, pi.currency)); 
    }
    if let Some(canceled) = pi.canceled_at { 
        fields.push(format!("*Canceled At:* {}", format_timestamp(canceled))); 
    }

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Payout handling -----

fn payout_to_slack_event(po: &Payout, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&po.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* payout - *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Payout ID:* {}", po.id));
    fields.push(format!("*Amount:* {:.2} {}", po.amount as f64 / 100.0, po.currency));
    fields.push(format!("*Status:* {}", human_event_type(&po.status)));
    fields.push(format!("*Arrival Date:* {}", format_timestamp(po.arrival_date)));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Source handling -----

fn source_to_slack_event(so: &Source, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&so.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* source - *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Source ID:* {}", so.id));
    if let Some(amount) = so.amount { 
        fields.push(format!("*Amount:* {:.2} {}", amount as f64 / 100.0, so.currency.clone().unwrap_or("USD".into()))); 
    }
    fields.push(format!("*Status:* {}", so.status));
    fields.push(format!("*Type:* {}", human_event_type(&so.type_)));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Subscription Schedule handling -----

fn subscriptionschedule_to_slack_event(ss: &SubscriptionSchedule, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&ss.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* subscription schedule - *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Schedule ID:* {}", ss.id));
    fields.push(format!("*Customer:* {}", ss.customer));
    fields.push(format!("*Status:* {}", human_event_type(&ss.status)));
    fields.push(format!("*End Behavior:* {}", human_event_type(&ss.end_behavior)));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Topup handling -----

fn topup_to_slack_event(tu: &Topup, livemode: bool) -> SlackEvent {
    let event_clean = human_event_type(&tu.event_type);
    let memo = if !livemode { " :memo:" } else { "" };
    let title = format!("New *Stripe* topup - *{}*{}", event_clean, memo);

    let title_block = SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: title.clone() },
    });

    let mut fields = Vec::new();
    fields.push(format!("*Topup ID:* {}", tu.id));
    fields.push(format!("*Amount:* ${:.2} {}", tu.amount as f64 / 100.0, tu.currency));
    fields.push(format!("*Status:* {}", human_event_type(&tu.status)));

    let field_objs: FieldsSectionFields = fields.into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    let fields_block = SlackEventUntagged::FieldsSection(
        FieldsSection { type_: FieldsSectionType::Section, fields: field_objs }
    );

    SlackEvent { blocks: vec![title_block, fields_block] }
}

// ----- Helper functions -----

/// Convert any `Debug`-printable event into a human-readable string.
fn human_event_type<E: std::fmt::Debug>(ev: &E) -> String {
    let raw_str = format!("{:?}", ev);
    let last = raw_str.rsplit("::").next().unwrap_or(&raw_str);
    // Generic: strip known prefixes
    let raw = last
        .trim_start_matches("Invoiceitem")
        .trim_start_matches("Invoice")
        .trim_start_matches("Charge")
        .trim_start_matches("Customer")
        .trim_start_matches("IssuingAuthorization")
        .trim_start_matches("IssuingCardholder")
        .trim_start_matches("IssuingCard")
        .trim_start_matches("IssuingDispute")
        .trim_start_matches("PaymentIntent")
        .trim_start_matches("Payout")
        .trim_start_matches("Source")
        .trim_start_matches("SubscriptionSchedule")
        .trim_start_matches("Topup");
    // Capitalize first letter and keep CamelCase
    let mut chars = raw.chars();
    if let Some(first) = chars.next() {
        let mut human = first.to_uppercase().collect::<String>();
        human.push_str(chars.as_str());
        human
    } else {
        raw.to_string()
    }
}

fn format_timestamp(ts: i32) -> String {
    let dt = Utc
        .timestamp_opt(ts as i64, 0)
        .single()
        .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
    dt.format("%b %d, %Y").to_string()
}

fn format_invoice_items(lines: &[LineItem]) -> String {
    if lines.is_empty() {
        "-".into()
    } else {
        lines
            .iter()
            .map(|l| format!("- {} ({:.2} {})", l.description, l.amount as f64 / 100.0, l.currency))
            .collect::<Vec<_>>()
            .join("\n")
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_timestamp() {
        let ts = 1633036800;
        assert_eq!(format_timestamp(ts), "Sep 30, 2021");
        assert_eq!(format_timestamp(0), "Jan 01, 1970");
    }

    #[test]
    fn test_format_invoice_items_empty() {
        let empty: InvoiceLines = Vec::new();
        assert_eq!(format_invoice_items(&empty), "-");
    }

    #[test]
    fn test_format_invoice_items_nonempty() {
        let items = vec![
            LineItem { description: "A".into(), amount: 1200, currency: "USD".into() },
            LineItem { description: "B".into(), amount: 800, currency: "EUR".into() },
        ];
        let out = format_invoice_items(&items);
        assert!(out.contains("- A (12.00 USD)"));
        assert!(out.contains("- B (8.00 EUR)"));
    }

    #[test]
    fn test_human_event_type() {
        assert_eq!(human_event_type(&InvoiceEventType::InvoiceCreated), "Created");
        assert_eq!(human_event_type(&InvoiceEventType::InvoiceFinalized), "Finalized");
        assert_eq!(human_event_type(&ChargeEventType::ChargeCaptured), "Captured");
        assert_eq!(human_event_type(&ChargeEventType::ChargeExpired), "Expired");
        assert_eq!(human_event_type(&CustomerEventType::CustomerCreated), "Created");
        assert_eq!(human_event_type(&InvoiceitemEventType::InvoiceitemCreated), "Created");
        assert_eq!(human_event_type(&IssuingAuthorizationEventType::IssuingAuthorizationCreated), "Created");
        assert_eq!(human_event_type(&IssuingCardEventType::IssuingCardCreated), "Created");
        assert_eq!(human_event_type(&IssuingCardholderEventType::IssuingCardholderUpdated), "Updated");
        assert_eq!(human_event_type(&IssuingDisputeEventType::IssuingDisputeFundsReinstated), "FundsReinstated");
        assert_eq!(human_event_type(&PaymentIntentEventType::PaymentIntentPartiallyFunded), "PartiallyFunded");
        assert_eq!(human_event_type(&PayoutEventType::PayoutCanceled), "Canceled");
    }

    #[test]
    fn test_invoice_to_slack_event() {
        let inv = Invoice { /* populate required fields */
            account_country: Some("US".into()),
            account_name: Some("Acct".into()),
            amount_due: 1500,
            amount_paid: 1500,
            amount_remaining: 0,
            amount_shipping: 0,
            attempt_count: 1,
            attempted: false,
            billing_reason: None,
            collection_method: InvoiceCollectionMethod::ChargeAutomatically,
            created: 1609459200,
            currency: "USD".into(),
            customer: None,
            customer_email: Some("c@e.com".into()),
            customer_name: Some("Cust".into()),
            event_type: InvoiceEventType::InvoiceCreated,
            hosted_invoice_url: None,
            id: None,
            lines: vec![LineItem { description: "Svc".into(), amount: 1500, currency: "USD".into() }],
            paid: true,
            paid_out_of_band: false,
            period_end: 1609545600,
            period_start: 1609459200,
            status: Some(InvoiceStatus::Paid),
            subtotal: 1500,
            total: 1500,
        };
        let ev = invoice_to_slack_event(&inv, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[0] {
            assert!(ts.text.text.contains("Created"));
            assert!(ts.text.text.contains("Paid"));
        } else { panic!("Expected TextSection"); }
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount Due:*")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Items:*")));
        } else { panic!("Expected FieldsSection"); }
    }

    #[test]
    fn test_charge_to_slack_event() {
        let ch = Charge {
            amount: 2000,
            amount_captured: 1500,
            amount_refunded: 0,
            balance_transaction: Some("txn_123".into()),
            calculated_statement_descriptor: None,
            captured: true,
            created: 1610000000,
            currency: "USD".into(),
            customer: Some("cus_456".into()),
            description: Some("Test charge".into()),
            disputed: false,
            event_type: ChargeEventType::ChargeCaptured,
            failure_code: None,
            failure_message: None,
            id: "ch_789".into(),
            invoice: None,
            paid: true,
            receipt_url: Some("https://receipt.url".into()),
            refunded: false,
            status: ChargeStatus::Succeeded,
        };
        let ev = charge_to_slack_event(&ch, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[0] {
            assert!(ts.text.text.contains("Captured"));
            assert!(ts.text.text.contains("Succeeded"));
        } else { panic!("Expected TextSection"); }
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Charge ID:* ch_789")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 20.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Customer:* cus_456")));
        } else { panic!("Expected FieldsSection"); }
    }

    #[test]
    fn test_customer_to_slack_event() {
        let c = Customer {
            id: "cus_001".into(),
            event_type: CustomerEventType::CustomerCreated,
            name: Some("Test User".into()),
            email: Some("test@example.com".into()),
            description: Some("VIP customer".into()),
            address: None,
            balance: None,
            currency: None,
            delinquent: None,
            invoice_prefix: None,
            next_invoice_sequence: None,
            phone: None,
            created: 1620000000,
        };
        let ev = customer_to_slack_event(&c, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[0] {
            assert!(ts.text.text.contains("customer – *Created*"));
        } else {
            panic!("Expected TextSection");
        }
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Customer ID:* cus_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Name:* Test User")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Email:* test@example.com")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_invoiceitem_to_slack_event() {
        let ii = Invoiceitem {
            id: "ii_001".into(),
            event_type: InvoiceitemEventType::InvoiceitemCreated,
            amount: 500,
            currency: "USD".into(),
            customer: "cus_001".into(),
            date: 1625000000,
            description: Some("Item desc".into()),
            period: Period { start: 1624000000, end: 1625000000 },
            quantity: 3,
        };
        let ev = invoiceitem_to_slack_event(&ii, false);
        assert_eq!(ev.blocks.len(), 2);
        // Check title block contains human-readable event
        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[0] {
            println!("{}", ts.text.text);
            assert!(ts.text.text.contains("*Created*"));
        } else {
            panic!("Expected TextSection");
        }
        // Check fields block
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Item ID:* ii_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 5.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Quantity:* 3")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Date:*")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_issuingauthorization_to_slack_event() {
        let ia = IssuingAuthorization {
            id: "ia_001".into(),
            amount: 2500,
            amount_details: None,
            approved: true,
            authorization_method: IssuingAuthorizationAuthorizationMethod::Online,
            card: "card_123".into(),
            cardholder: None,
            created: 1627000000,
            currency: "USD".into(),
            event_type: IssuingAuthorizationEventType::IssuingAuthorizationCreated,
            merchant_amount: 2400,
            merchant_currency: "USD".into(),
            merchant_data: MerchantData {
                category: "Retail".into(),
                category_code: "5812".into(),
                city: None,
                country: None,
                name: Some("Store".into()),
                network_id: "net_1".into(),
                postal_code: None,
                state: None,
                tax_id: None,
                terminal_id: None,
                url: None,
            },
            status: IssuingAuthorizationStatus::Pending,
            wallet: None,
        };
        let ev = issuingauthorization_to_slack_event(&ia, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Authorization ID:* ia_001")));
        } else {
            panic!("Expected FieldsSection");
        }
    } 

    #[test]
    fn test_issuingcard_to_slack_event() {
        let ic = IssuingCard {
            id: "ic_001".into(),
            brand: "Visa".into(),
            last4: "1234".into(),
            status: IssuingCardStatus::Active,
            type_: IssuingCardType::Physical,
            exp_month: 12,
            exp_year: 2025,
            cancellation_reason: None,
            cardholder: IssuingCardCardholder { email: None, id: None },
            created: 1627000000,
            currency: "USD".into(),
            cvc: None,
            event_type: IssuingCardEventType::IssuingCardCreated,
            financial_account: None,
        };
        let ev = issuingcard_to_slack_event(&ic, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Card ID:* ic_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Brand:* Visa")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Last4:* 1234")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_issuingcardholder_to_slack_event() {
        let ih = IssuingCardholder {
            id: "ih_001".into(),
            name: "Alice".into(),
            email: Some("alice@example.com".into()),
            status: IssuingCardholderStatus::Active,
            phone_number: None,
            billing: Address { city: None, country: None, line1: None, line2: None, postal_code: None, state: None },
            event_type: IssuingCardholderEventType::IssuingCardholderCreated,
            individual: None,
            created: 1627100000,
            type_: IssuingCardholderType::Individual,
        };
        let ev = issuingcardholder_to_slack_event(&ih, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!(">> issuingcardholder\n ---\n{}\n---\n", 
                fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n")
            );
            assert!(fs.fields.iter().any(|f| f.text.contains("*Cardholder ID:* ih_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Name:* Alice")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* Active")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_issuingdispute_to_slack_event() {
        let idp = IssuingDispute {
            id: "idp_001".into(),
            amount: 750,
            currency: "USD".into(),
            event_type: IssuingDisputeEventType::IssuingDisputeCreated,
            loss_reason: None,
            reason: IssuingDisputeReason::Other,
            status: IssuingDisputeStatus::Submitted,
            created: 1627200000,
        };
        let ev = issuingdispute_to_slack_event(&idp, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Dispute ID:* idp_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 7.50 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Reason:* Other")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* Submitted")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_paymentintent_to_slack_event() {
        let pi = PaymentIntent {
            id: "pi_001".into(),
            amount: 12345,
            amount_received: Some(5000),
            canceled_at: Some(1628000000),
            cancellation_reason: Some(PaymentIntentCancellationReason::Abandoned),
            capture_method: PaymentIntentCaptureMethod::Manual,
            confirmation_method: PaymentIntentConfirmationMethod::Automatic,
            created: 1628000000,
            currency: "USD".into(),
            customer: Some("cus_XYZ".into()),
            description: Some("Test payment".into()),
            event_type: PaymentIntentEventType::PaymentIntentCreated,
            invoice: Some("inv_001".into()),
            payment_method_types: vec!["card".into()],
            receipt_email: Some("test@ex.com".into()),
            status: PaymentIntentStatus::RequiresConfirmation,
        };
        let ev = paymentintent_to_slack_event(&pi, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Intent ID:* pi_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 123.45 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* RequiresConfirmation")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Received:* 50.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Canceled At:*")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_payout_to_slack_event() {
        let po = Payout {
            id: "po_001".into(),
            amount: 3500,
            arrival_date: 1629000000,
            automatic: true,
            balance_transaction: None,
            created: 1629000000,
            currency: "USD".into(),
            description: Some("Test payout".into()),
            event_type: PayoutEventType::PayoutCreated,
            failure_code: None,
            failure_message: None,
            method: "standard".into(),
            reconciliation_status: PayoutReconciliationStatus::Completed,
            source_type: "bank_account".into(),
            statement_descriptor: None,
            status: "Paid".into(),
            type_: PayoutType::BankAccount,
        };
        let ev = payout_to_slack_event(&po, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            println!("{}", fs.fields.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("\n"));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Payout ID:* po_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 35.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* \"Paid\"")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Arrival Date:*")));
        } else {
            panic!("Expected FieldsSection");
        }
    }

    #[test]
    fn test_source_to_slack_event() {
        let so = Source {
            id: "so_001".into(),
            amount: Some(1200),
            client_secret: "secret".into(),
            created: 1629000000,
            currency: Some("USD".into()),
            customer: Some("cus_001".into()),
            event_type: SourceEventType::SourceChargeable,
            owner: None,
            statement_descriptor: None,
            status: "chargeable".into(),
            type_: SourceType::Card,
        };
        let ev = source_to_slack_event(&so, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            assert!(fs.fields.iter().any(|f| f.text.contains("*Source ID:* so_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* 12.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* chargeable")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Type:* Card")));
        } else { panic!("Expected FieldsSection"); }
    }

    #[test]
    fn test_subscriptionschedule_to_slack_event() {
        let ss = SubscriptionSchedule {
            id: "ss_001".into(),
            customer: "cus_002".into(),
            default_settings: SubscriptionDefaultSettings { billing_cycle_anchor: SubscriptionDefaultSettingsBillingCycleAnchor::Automatic, collection_method: None },
            end_behavior: SubscriptionScheduleEndBehavior::Cancel,
            event_type: SubscriptionScheduleEventType::SubscriptionScheduleCreated,
            released_at: None,
            canceled_at: None,
            completed_at: None,
            created: 1629000000,
            status: SubscriptionScheduleStatus::Active,
        };
        let ev = subscriptionschedule_to_slack_event(&ss, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            assert!(fs.fields.iter().any(|f| f.text.contains("*Schedule ID:* ss_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Customer:* cus_002")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* Active")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*End Behavior:* Cancel")));
        } else { panic!("Expected FieldsSection"); }
    }

    #[test]
    fn test_topup_to_slack_event() {
        let tu = Topup {
            id: "tu_001".into(),
            amount: 6000,
            created: 1629000000,
            currency: "USD".into(),
            description: Some("Test topup".into()),
            event_type: TopupEventType::TopupCreated,
            expected_availability_date: None,
            failure_code: None,
            failure_message: None,
            status: TopupStatus::Pending,
        };
        let ev = topup_to_slack_event(&tu, false);
        assert_eq!(ev.blocks.len(), 2);
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[1] {
            assert!(fs.fields.iter().any(|f| f.text.contains("*Topup ID:* tu_001")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Amount:* $60.00 USD")));
            assert!(fs.fields.iter().any(|f| f.text.contains("*Status:* Pending")));
        } else { panic!("Expected FieldsSection"); }
    }

    #[test]
    fn test_stripe_to_slack_dispatch() -> Result<()> {
        // Invoice dispatch
        let inv = Invoice { 
            account_country: None, account_name: None, amount_due: 0, amount_paid: 0, amount_remaining: 0, amount_shipping: 0, attempt_count: 0, attempted: false, billing_reason: None, collection_method: InvoiceCollectionMethod::SendInvoice, created: 0, currency: "USD".into(), customer: None, customer_email: None, customer_name: None, event_type: InvoiceEventType::InvoiceCreated, hosted_invoice_url: None, id: None, lines: vec![], paid: false, paid_out_of_band: false, period_end: 0, period_start: 0, status: None, subtotal: 0, total: 0,
        };
        let se_inv = StripeEvent {
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Invoice(inv), livemode: true, 
        };
        stripe_to_slack(se_inv)?;
    
        // Invoiceitem dispatch
        let ii = Invoiceitem { 
            id: "".into(), event_type: InvoiceitemEventType::InvoiceitemDeleted, amount: 0, currency: "".into(), customer: "".into(), date: 0, description: None, period: Period { start: 0, end: 0 }, quantity: 0, 
        };
        let se_ii = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Invoiceitem(ii), livemode: true, 
        };
        stripe_to_slack(se_ii)?;
    
        // Charge dispatch
        let ch = Charge { 
            amount: 0, amount_captured: 0, amount_refunded: 0, balance_transaction: None, calculated_statement_descriptor: None, captured: false, created: 0, currency: "USD".into(), customer: None, description: None, disputed: false, event_type: ChargeEventType::ChargeFailed, failure_code: None, failure_message: None, id: "".into(), invoice: None, paid: false, receipt_url: None, refunded: false, status: ChargeStatus::Failed, 
        };
        let se_ch = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Charge(ch), livemode: true, 
        };
        stripe_to_slack(se_ch)?;
    
        // Customer dispatch
        let c = Customer { 
            id: "".into(), event_type: CustomerEventType::CustomerUpdated, name: None, email: None, description: None, address: None, balance: None, currency: None, delinquent: None, invoice_prefix: None, next_invoice_sequence: None, phone: None, created: 0, 
        };
        let se_c = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Customer(c), livemode: true, 
        };
        stripe_to_slack(se_c)?;
    
        // IssuingAuthorization dispatch
        let ia = IssuingAuthorization { 
            id: "".into(), amount: 0, amount_details: None, approved: false, authorization_method: IssuingAuthorizationAuthorizationMethod::Swipe, card: "".into(), cardholder: None, created: 0, currency: "".into(), event_type: IssuingAuthorizationEventType::IssuingAuthorizationUpdated, merchant_amount: 0, merchant_currency: "".into(), merchant_data: MerchantData { category: "".into(), category_code: "".into(), city: None, country: None, name: None, network_id: "".into(), postal_code: None, state: None, tax_id: None, terminal_id: None, url: None, }, status: IssuingAuthorizationStatus::Closed, wallet: None, 
        };
        let se_ia = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::IssuingAuthorization(ia), livemode: true, 
        };
        stripe_to_slack(se_ia)?;
    
        // IssuingCard dispatch
        let ic = IssuingCard { 
            id: "ic_001".into(), brand: "Visa".into(), last4: "1234".into(), status: IssuingCardStatus::Active, type_: IssuingCardType::Physical, exp_month: 12, exp_year: 2025, cancellation_reason: None, cardholder: IssuingCardCardholder { email: None, id: None }, created: 1627000000, currency: "USD".into(), cvc: None, event_type: IssuingCardEventType::IssuingCardCreated, financial_account: None, 
        };
        let se_ic = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::IssuingCard(ic), livemode: true, 
        };
        stripe_to_slack(se_ic)?;
    
        // IssuingCardholder dispatch
        let ih = IssuingCardholder { 
            id: "".into(), name: "".into(), email: None, status: IssuingCardholderStatus::Active, phone_number: None, billing: Address { city: None, country: None, line1: None, line2: None, postal_code: None, state: None, }, event_type: IssuingCardholderEventType::IssuingCardholderCreated, individual: None, created: 0, type_: IssuingCardholderType::Individual, 
        };
        let se_ih = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::IssuingCardholder(ih), livemode: true, 
        };
        stripe_to_slack(se_ih)?;
    
        // IssuingDispute dispatch
        let idp = IssuingDispute { 
            id: "".into(), amount: 0, currency: "".into(), event_type: IssuingDisputeEventType::IssuingDisputeCreated, loss_reason: None, reason: IssuingDisputeReason::Other, status: IssuingDisputeStatus::Submitted, created: 0, 
        };
        let se_idp = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::IssuingDispute(idp), livemode: true, 
        };
        stripe_to_slack(se_idp)?;
    
        // PaymentIntent dispatch
        let pi = PaymentIntent { 
            id: "".into(), amount: 0, amount_received: None, canceled_at: None, cancellation_reason: None, capture_method: PaymentIntentCaptureMethod::Automatic, confirmation_method: PaymentIntentConfirmationMethod::Automatic, created: 0, currency: "".into(), customer: None, description: None, event_type: PaymentIntentEventType::PaymentIntentCreated, invoice: None, payment_method_types: vec![], receipt_email: None, status: PaymentIntentStatus::RequiresPaymentMethod, 
        };
        let se_pi = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::PaymentIntent(pi), livemode: true, 
        };
        stripe_to_slack(se_pi)?;
    
        // Payout dispatch
        let po = Payout { 
            id: "".into(), amount: 0, arrival_date: 0, automatic: false, balance_transaction: None, created: 0, currency: "".into(), description: None, event_type: PayoutEventType::PayoutCreated, failure_code: None, failure_message: None, method: "".into(), reconciliation_status: PayoutReconciliationStatus::Completed, source_type: "".into(), statement_descriptor: None, status: "".into(), type_: PayoutType::BankAccount, 
        };
        let se_po = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Payout(po), livemode: true, 
        };
        stripe_to_slack(se_po)?;
    
        // Source dispatch
        let so = Source { 
            id: "".into(), amount: None, client_secret: "".into(), created: 0, currency: None, customer: None, event_type: SourceEventType::SourceChargeable, owner: None, statement_descriptor: None, status: "".into(), type_: SourceType::Card, 
        };
        let se_so = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Source(so), livemode: true, 
        };
        stripe_to_slack(se_so)?;
    
        // SubscriptionSchedule dispatch
        let ss = SubscriptionSchedule { 
            id: "".into(), customer: "".into(), default_settings: SubscriptionDefaultSettings { billing_cycle_anchor: SubscriptionDefaultSettingsBillingCycleAnchor::Automatic, collection_method: None, }, end_behavior: SubscriptionScheduleEndBehavior::Cancel, event_type: SubscriptionScheduleEventType::SubscriptionScheduleCreated, released_at: None, canceled_at: None, completed_at: None, created: 0, status: SubscriptionScheduleStatus::Active, 
        };
        let se_ss = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::SubscriptionSchedule(ss), livemode: true, 
        };
        stripe_to_slack(se_ss)?;
    
        // Topup dispatch
        let tu = Topup { 
            id: "".into(), amount: 0, created: 0, currency: "".into(), description: None, event_type: TopupEventType::TopupCreated, expected_availability_date: None, failure_code: None, failure_message: None, status: TopupStatus::Pending, 
        };
        let se_tu = StripeEvent { 
            api_version: None, created: 0, fluvio_version: String::new(), id: String::new(), pending_webhooks: 0, data: EventData::Topup(tu), livemode: true, 
        };
        stripe_to_slack(se_tu)?;
    
        Ok(())
    }
}
