use sdfg::Result;
use sdfg::sdf;
use crate::bindings::infinyon::stripe_slack_types::types::SlackEvent;
use crate::bindings::infinyon::stripe_slack_types::types::StripeEvent;
#[allow(unused_imports)]
use crate::bindings::infinyon::stripe_slack_types::types::*;

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
    let header = format_header(&humalize_event_type(&inv.event_type), Some(":moneybag:"), livemode);
    let account_fields = vec![
        format!("*Account:*\n {} ({})", 
            inv.account_name.clone().unwrap_or_default(), 
            inv.account_country.clone().unwrap_or_default()
        ),
        format!("*Customer:*\n {} <{}>", 
            inv.customer_name.clone().unwrap_or_default(), 
            inv.customer_email.clone().unwrap_or_default()
        ),
    ];
    let amount_fields = vec![
        format!("*Amount Due:*\n {}", 
            format_money(inv.amount_due.into(), &inv.currency)),
        format!("*Amount Paid:*\n {}", 
            format_money(inv.amount_paid.into(), &inv.currency)),
    ];
    let period_text = format!("*Period:*\n {} – {}", 
        format_timestamp(inv.period_start), 
        format_timestamp(inv.period_end)
    );
    let itemized_text = format!("*Itemized:*{}", 
        format_invoice_items(&inv.lines)
    );
    let status_text = format!("*Status:*\n {}", 
        get_enum_text(&inv.status.as_ref().map(|s| format!("{:?}", s)).unwrap_or_default())
    );

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(account_fields),
        make_text_section(period_text),
        make_fields_section(amount_fields),
        make_text_section(itemized_text),
        make_text_section(status_text)
    ];

    SlackEvent{ blocks }
}

// ----- Invoiceitem handling -----

fn invoiceitem_to_slack_event(ii: &Invoiceitem, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ii.event_type), Some(":memo:"), livemode);
    let fields = vec![
        format!("*Item ID:*\n {}", ii.id),
        format!("*Amount:*\n {}", format_money(ii.amount.into(), &ii.currency)),
        format!("*Quantity:*\n {}", ii.quantity),
        format!("*Date:*\n {}", format_timestamp(ii.date))
    ];

    // Build known blocks
    let mut blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];

    // Add optional blocks
    if let Some(desc) = &ii.description {
        blocks.push(make_text_section(format!("*Description:*\n {}", desc)));
    }

    SlackEvent{ blocks }
}

// ----- Charge handling -----

fn charge_to_slack_event(ch: &Charge, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ch.event_type), Some(":credit_card:"), livemode);
    let mut fields = vec![
        format!("*Charge ID:*\n {}", ch.id),
        format!("*Amount:*\n {}", format_money(ch.amount.into(), &ch.currency)),
        format!("*Description:*\n {}", ch.description.clone().unwrap_or_default()),
        format!("*Status:*\n {}", get_enum_text(format!("{:?}", ch.status).as_str())),
    ];
    if let Some(cust) = &ch.customer {
        fields.push(format!("*Customer:*\n {}", cust));
    }

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Customer handling -----

fn customer_to_slack_event(c: &Customer, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&c.event_type), Some(":office_worker:"), livemode);    
    let mut fields = vec![
        format!("*Customer ID:*\n {}", c.id),
    ];
    if let Some(name) = &c.name {
        fields.push(format!("*Name:*\n {}", name));
    }
    if let Some(email) = &c.email {
        fields.push(format!("*Email:*\n {}", email));
    }
    
    // Build known blocks
    let mut blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];

    // Add optional blocks
    if let Some(desc) = &c.description {
        blocks.push(make_text_section(format!("*Description:*\n {}", desc)));
    }

    SlackEvent{ blocks }
}

// ----- Issuing Authorization handling -----

fn issuingauthorization_to_slack_event(ia: &IssuingAuthorization, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ia.event_type), Some(":ok:"), livemode);

    let fields = vec![
        format!("*Authorization ID:*\n {}", ia.id),
        format!("*Amount:*\n {}", format_money(ia.amount.into(), &ia.currency)),
        format!("*Merchant Amount:*\n {}", format_money(ia.merchant_amount.into(), &ia.merchant_currency)),
        format!("*Card:*\n {}", ia.card),
        format!("*Approved:*\n {}", ia.approved),
        format!("*Status:*\n {}",  get_enum_text(format!("{:?}", ia.status).as_str())),
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Issuing Card handling -----

fn issuingcard_to_slack_event(ic: &IssuingCard, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ic.event_type), Some(":card_index:"), livemode);    

    let fields = vec![
        format!("*Card ID:*\n {}", ic.id),
        format!("*Brand:*\n {}", ic.brand),
        format!("*Last4:*\n {}", ic.last4),
        format!("*Type:*\n {}", get_enum_text(format!("{:?}", ic.type_).as_str())),
        format!("*Exp:*\n {}/{}", ic.exp_month, ic.exp_year),
        format!("*Status:*\n {}", get_enum_text(format!("{:?}", ic.status).as_str())),
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Issuing Cardholder handling -----

fn issuingcardholder_to_slack_event(ih: &IssuingCardholder, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ih.event_type), Some(":envelope_with_arrow:"), livemode);      
    
    let fields = vec![
        format!("*Cardholder ID:*\n {}", ih.id),
        format!("*Name:*\n {}", ih.name),
        format!("*Email:*\n {}", ih.email.clone().unwrap_or_default()),
        format!("*Status:*\n {}",  get_enum_text(format!("{:?}", ih.status).as_str())),
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Issuing Dispute handling -----

fn issuingdispute_to_slack_event(idp: &IssuingDispute, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&idp.event_type), Some(":warning:"), livemode);      

    let mut fields = vec![
        format!("*Dispute ID:*\n {}", idp.id),
        format!("*Amount:*\n {}", format_money(idp.amount.into(), &idp.currency)),
        format!("*Reason:*\n {}", get_enum_text(format!("{:?}", idp.reason).as_str())),
        format!("*Status:*\n {}", get_enum_text(format!("{:?}", idp.status).as_str())),
    ];
    if let Some(lr) = &idp.loss_reason {
        fields.push(format!("*Loss Reason:*\n {}", get_enum_text(format!("{:?}",lr).as_str())));
    }

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Payment Intent handling -----

fn paymentintent_to_slack_event(pi: &PaymentIntent, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&pi.event_type), Some(":money_with_wings:"), livemode);      

    let mut fields = vec![
        format!("*Intent ID:*\n {}", pi.id),
        format!("*Amount:*\n {}", format_money(pi.amount.into(), &pi.currency)),
        format!("*Status:*\n {}", get_enum_text(format!("{:?}", pi.status).as_str())),
    ];
    if let Some(received) = pi.amount_received {
        fields.push(format!("*Received:*\n {}", format_money(received.into(), &pi.currency)));
    }
    if let Some(canceled) = pi.canceled_at {
        fields.push(format!("*Canceled At:*\n {}", format_timestamp(canceled)));
    }

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Payout handling -----

fn payout_to_slack_event(po: &Payout, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&po.event_type), Some(":hand_with_index_finger:"), livemode);      

    let fields = vec![
        format!("*Payout ID:*\n {}", po.id),
        format!("*Amount:*\n {}", format_money(po.amount.into(), &po.currency)),
        format!("*Status:*\n {}", po.status),
        format!("*Arrival Date:*\n {}", format_timestamp(po.arrival_date)),
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Source handling -----

fn source_to_slack_event(so: &Source, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&so.event_type), Some(":office:"), livemode);      
    let mut fields = vec![
        format!("*Source ID:*\n {}", so.id),
        format!("*Type:*\n {}", get_enum_text(format!("{:?}", so.type_).as_str())),
        format!("*Status:*\n {}", so.status),
    ];
    if let Some(amount) = so.amount {
        fields.push(format!("*Amount:*\n {}", 
            format_money(amount.into(), &so.currency.clone().unwrap_or_else(|| "USD".into()))));
    }

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Subscription Schedule handling -----

fn subscriptionschedule_to_slack_event(ss: &SubscriptionSchedule, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&ss.event_type), Some(":incoming_envelope:"), livemode);    
    let fields = vec![
        format!("*Schedule ID:*\n {}", ss.id),
        format!("*Customer:*\n {}", ss.customer),
        format!("*End Behavior:*\n {}",get_enum_text(format!("{:?}", ss.end_behavior).as_str())),
        format!("*Status:*\n {}",  get_enum_text(format!("{:?}", ss.status).as_str())),
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Topup handling -----

fn topup_to_slack_event(tu: &Topup, livemode: bool) -> SlackEvent {
    let header = format_header(&humalize_event_type(&tu.event_type), Some(":top:"), livemode);    
    let fields = vec![
        format!("*Topup ID:*\n {}", tu.id),
        format!("*Amount:*\n {}", format_money(tu.amount.into(), &tu.currency)),
        format!("*Status:*\n {}", get_enum_text(format!("{:?}", tu.status).as_str()))
    ];

    // Build known blocks
    let blocks = vec![
        make_header(header),
        make_divider(),
        make_fields_section(fields)
    ];
    
    SlackEvent{ blocks }
}

// ----- Helper functions -----

/// Convert any `Debug`-printable event into a human-readable string.
fn humalize_event_type<E: std::fmt::Debug>(ev: &E) -> String {
    let raw_str = format!("{:?}", ev);
    let raw = raw_str.rsplit("::").next().unwrap_or(&raw_str);

    // Split by uppercase letters and join them with a space
    raw.chars()
        .enumerate()
        .map(|(i, c)| {
            if i > 0 && c.is_uppercase() {
                format!(" {}", c)
            } else {
                c.to_string()
            }
        })
        .collect::<String>()
}

/// Formats a Slack title for any service event.
fn format_header(event: &str, emoji: Option<&str>, livemode: bool) -> String {
    let mut header = "".to_string();
    
    if let Some(emoji) = emoji {
        header.push_str(&format!("{} ", emoji));
    }
    header.push_str(&format!("{}", event));
    if livemode {
        header.push_str(" - :white_check_mark:");
    } else {
        header.push_str(" - :ghost: demo :ghost:");
    }

    header
}

/// Stripe before "::" or return original
fn get_enum_text(input: &str) -> String {
    if let Some(index) = input.find("::") {
        input[index + 2..].to_string()
    } else {
        input.to_string()
    }
}

fn format_money(amount: i64, currency: &str) -> String {
    format!("{:.2} {}", amount as f64 / 100.0, currency)
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
        "\n - ".into()
    } else {
        lines
            .iter()
            .map(|l| format!("\n • {} ({:.2} {})", l.description, l.amount as f64 / 100.0, l.currency))
            .collect::<Vec<_>>()
            .join("")
    }
}

fn make_text_section<T: Into<String>>(text: T) -> SlackEventUntagged {
    SlackEventUntagged::TextSection(TextSection {
        type_: TextSectionType::Section,
        text: TextObject { type_: TextObjectType::Mrkdwn, text: text.into() },
    })
}

fn make_fields_section(raw_fields: Vec<String>) -> SlackEventUntagged {
    let fields = raw_fields
        .into_iter()
        .map(|t| TextObject { type_: TextObjectType::Mrkdwn, text: t })
        .collect();
    SlackEventUntagged::FieldsSection(FieldsSection {
        type_: FieldsSectionType::Section,
        fields,
    })
}

fn make_header<T: Into<String>>(text: T) -> SlackEventUntagged {
    SlackEventUntagged::Header(Header {
        type_: HeaderType::Header,
        text: TextObject { type_: TextObjectType::PlainText, text: text.into() },
    })
}

fn make_divider() -> SlackEventUntagged {
    SlackEventUntagged::Divider(Divider {
        type_: DividerType::Divider
    })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_humalize_event_type() {
        assert_eq!(humalize_event_type(&InvoiceEventType::InvoiceCreated), 
            "Invoice Created");
        assert_eq!(humalize_event_type(&InvoiceEventType::InvoiceFinalized), 
            "Invoice Finalized");
        assert_eq!(humalize_event_type(&ChargeEventType::ChargeCaptured), 
            "Charge Captured");
        assert_eq!(humalize_event_type(&ChargeEventType::ChargeExpired), 
            "Charge Expired");
        assert_eq!(humalize_event_type(&CustomerEventType::CustomerCreated), 
            "Customer Created");
        assert_eq!(humalize_event_type(&InvoiceitemEventType::InvoiceitemCreated), 
            "Invoiceitem Created");
        assert_eq!(humalize_event_type(&IssuingAuthorizationEventType::IssuingAuthorizationCreated), 
            "Issuing Authorization Created");
        assert_eq!(humalize_event_type(&IssuingCardEventType::IssuingCardCreated), 
            "Issuing Card Created");
        assert_eq!(humalize_event_type(&IssuingCardholderEventType::IssuingCardholderUpdated), 
            "Issuing Cardholder Updated");
        assert_eq!(humalize_event_type(&IssuingDisputeEventType::IssuingDisputeFundsReinstated), 
            "Issuing Dispute Funds Reinstated");
        assert_eq!(humalize_event_type(&PaymentIntentEventType::PaymentIntentPartiallyFunded), 
            "Payment Intent Partially Funded");
        assert_eq!(humalize_event_type(&PayoutEventType::PayoutCanceled), 
            "Payout Canceled");
    }

    #[test]
    fn test_get_enum_text() {
        // Test case 1: String with "::"
        let input = "Stripe::PaymentIntentCreated";
        let result = get_enum_text(input);
        let expected = "PaymentIntentCreated";
        assert_eq!(result, expected);

        // Test case 2: String without "::"
        let input = "PaymentIntentCreated";
        let result = get_enum_text(input);
        let expected = "PaymentIntentCreated";
        assert_eq!(result, expected);

        // Test case 3: String with multiple "::"
        let input = "Stripe::PaymentIntent::Created";
        let result = get_enum_text(input);
        let expected = "PaymentIntent::Created";  // It only strips the first "::"
        assert_eq!(result, expected);

        // Test case 4: Empty string
        let input = "";
        let result = get_enum_text(input);
        let expected = "";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_timestamp() {
        let ts = 1633036800;
        assert_eq!(format_timestamp(ts), "Sep 30, 2021");
        assert_eq!(format_timestamp(0), "Jan 01, 1970");
    }
    
    #[test]
    fn test_format_invoice_items() {
        // Non-empty
        let items = vec![
            LineItem { description: "A".into(), amount: 1200, currency: "USD".into() },
            LineItem { description: "B".into(), amount: 800, currency: "EUR".into() },
        ];
        let out = format_invoice_items(&items);
        assert_eq!(out, "\n • A (12.00 USD)\n • B (8.00 EUR)");

        // Empty
        let empty: InvoiceLines = Vec::new();
        assert_eq!(format_invoice_items(&empty), "\n - ");
    }

    #[test]
    fn test_format_header() {
        // Test case 1: Livemode is true
        let event_clean = "Payment Intent Created";
        let emoji = Some(":money_with_wings:");
        let livemode = true;

        let result = format_header(event_clean, emoji, livemode);
        let expected = ":money_with_wings: Payment Intent Created - :white_check_mark:";
        assert_eq!(result, expected);

        // Test case 2: Livemode is false
        let emoji = None;
        let livemode = false;
        
        let result = format_header(event_clean, emoji, livemode);
        let expected = "Payment Intent Created - :ghost: demo :ghost:";
        assert_eq!(result, expected);
        
        // Test case 3: Different event and status
        let event_clean = "Invoice Created";
        let emoji = None;
        let livemode = true;
        
        let result = format_header(event_clean, emoji, livemode);
        let expected = "Invoice Created - :white_check_mark:";
        assert_eq!(result, expected);        
    }

    #[test]
    fn test_format_divider() {
        let section = make_divider();
        if let SlackEventUntagged::Divider(d) = section {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected Divider variant");
        }
    }

    #[test]
    fn test_invoice_to_slack_event() {
        let inv = Invoice {
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
            lines: vec![LineItem {
                description: "Svc".into(),
                amount: 1500,
                currency: "USD".into(),
            }],
            paid: true,
            paid_out_of_band: false,
            period_end: 1609545600,
            period_start: 1609459200,
            status: Some(InvoiceStatus::Paid),
            subtotal: 1500,
            total: 1500,
        };

        let ev = invoice_to_slack_event(&inv, false);
        assert_eq!(ev.blocks.len(), 7);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":moneybag: Invoice Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected block to be Divider");
        }
                
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 2);
            assert_eq!(texts[0], "*Account:*\n Acct (US)".to_string());
            assert_eq!(texts[1], "*Customer:*\n Cust <c@e.com>".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }

        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[3] {
            assert_eq!(ts.text.text, "*Period:*\n Jan 01, 2021 – Jan 02, 2021".to_string());
        } else {
            panic!("Expected block to be TextSection");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[4] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 2);
            assert_eq!(texts[0], "*Amount Due:*\n 15.00 USD".to_string());
            assert_eq!(texts[1], "*Amount Paid:*\n 15.00 USD".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }

        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[5] {
            assert_eq!(ts.text.text, "*Itemized:*\n • Svc (15.00 USD)".to_string());
        } else {
            panic!("Expected block to be TextSection");
        }
        
        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[6] {
            assert_eq!(ts.text.text, "*Status:*\n Paid".to_string());
        } else {
            panic!("Expected block to be TextSection");
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
        assert_eq!(ev.blocks.len(), 4);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":memo: Invoiceitem Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 4);
            assert_eq!(texts[0], "*Item ID:*\n ii_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 5.00 USD".to_string());
            assert_eq!(texts[2], "*Quantity:*\n 3".to_string());
            assert_eq!(texts[3], "*Date:*\n Jun 29, 2021".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }

        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[3] {
            assert_eq!(ts.text.text, "*Description:*\n Item desc".to_string());
        } else {
            panic!("Expected block to be TextSection");
        }
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text,":credit_card: Charge Captured - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            // You should have exactly these 5 fields
            assert_eq!(texts.len(), 5);
            assert_eq!(texts[0], "*Charge ID:*\n ch_789".to_string());
            assert_eq!(texts[1], "*Amount:*\n 20.00 USD".to_string());
            assert_eq!(texts[2], "*Description:*\n Test charge".to_string());
            assert_eq!(texts[3], "*Status:*\n Succeeded".to_string());
            assert_eq!(texts[4], "*Customer:*\n cus_456".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
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
        assert_eq!(ev.blocks.len(), 4);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":office_worker: Customer Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 3);
            assert_eq!(texts[0], "*Customer ID:*\n cus_001".to_string());
            assert_eq!(texts[1], "*Name:*\n Test User".to_string());
            assert_eq!(texts[2], "*Email:*\n test@example.com".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }

        if let SlackEventUntagged::TextSection(ts) = &ev.blocks[3] {
            assert_eq!(ts.text.text, "*Description:*\n VIP customer".to_string());
        } else {
            panic!("Expected block to be TextSection");
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":ok: Issuing Authorization Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 6);
            assert_eq!(texts[0], "*Authorization ID:*\n ia_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 25.00 USD".to_string());
            assert_eq!(texts[2], "*Merchant Amount:*\n 24.00 USD".to_string());
            assert_eq!(texts[3], "*Card:*\n card_123".to_string());
            assert_eq!(texts[4], "*Approved:*\n true".to_string());
            assert_eq!(texts[5], "*Status:*\n Pending".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":card_index: Issuing Card Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 6);
            assert_eq!(texts[0], "*Card ID:*\n ic_001".to_string());
            assert_eq!(texts[1], "*Brand:*\n Visa".to_string());
            assert_eq!(texts[2], "*Last4:*\n 1234".to_string());
            assert_eq!(texts[3], "*Type:*\n Physical".to_string());
            assert_eq!(texts[4], "*Exp:*\n 12/2025".to_string());
            assert_eq!(texts[5], "*Status:*\n Active".to_string());
        } else {
            panic!("Expected block to be FieldsSection");
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, 
                ":envelope_with_arrow: Issuing Cardholder Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 4);
            assert_eq!(texts[0], "*Cardholder ID:*\n ih_001".to_string());
            assert_eq!(texts[1], "*Name:*\n Alice".to_string());
            assert_eq!(texts[2], "*Email:*\n alice@example.com".to_string());
            assert_eq!(texts[3], "*Status:*\n Active".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
    }

    #[test]
    fn test_issuingdispute_to_slack_event() {
        let idp = IssuingDispute {
            id: "idp_001".into(),
            amount: 750,
            currency: "USD".into(),
            event_type: IssuingDisputeEventType::IssuingDisputeCreated,
            loss_reason: Some(IssuingDisputeLossReason::InvalidIncorrectAmountDispute),
            reason: IssuingDisputeReason::Other,
            status: IssuingDisputeStatus::Submitted,
            created: 1627200000,
        };
        let ev = issuingdispute_to_slack_event(&idp, false);
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text,":warning: Issuing Dispute Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 5);
            assert_eq!(texts[0], "*Dispute ID:*\n idp_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 7.50 USD".to_string());
            assert_eq!(texts[2], "*Reason:*\n Other".to_string());
            assert_eq!(texts[3], "*Status:*\n Submitted".to_string());
            assert_eq!(texts[4], "*Loss Reason:*\n InvalidIncorrectAmountDispute".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
    }

    #[test]
    fn test_paymentintent_to_slack_event() {
        let pi = PaymentIntent {
            id: "pi_001".into(),
            amount: 12345,
            amount_received: Some(5000),
            canceled_at: Some(1628000000),
            currency: "USD".into(),
            event_type: PaymentIntentEventType::PaymentIntentCreated,
            cancellation_reason: None,
            capture_method: PaymentIntentCaptureMethod::Manual,
            confirmation_method: PaymentIntentConfirmationMethod::Automatic,
            created: 1628000000,
            customer: Some("cus_XYZ".into()),
            description: Some("Test payment".into()),
            invoice: Some("inv_001".into()),
            payment_method_types: vec!["card".into()],
            receipt_email: Some("test@ex.com".into()),
            status: PaymentIntentStatus::RequiresConfirmation,
        };
        let ev = paymentintent_to_slack_event(&pi, false);
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, 
                ":money_with_wings: Payment Intent Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 5);
            assert_eq!(texts[0], "*Intent ID:*\n pi_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 123.45 USD".to_string());
            assert_eq!(texts[2], "*Status:*\n RequiresConfirmation".to_string());
            assert_eq!(texts[3], "*Received:*\n 50.00 USD".to_string());
            assert_eq!(texts[4], "*Canceled At:*\n Aug 03, 2021".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, 
                ":hand_with_index_finger: Payout Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 4);
            assert_eq!(texts[0], "*Payout ID:*\n po_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 35.00 USD".to_string());
            assert_eq!(texts[2], "*Status:*\n Paid".to_string());
            assert_eq!(texts[3], "*Arrival Date:*\n Aug 15, 2021".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
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
            status: "chargeable".to_string(),
            type_: SourceType::Card,
        };
        let ev = source_to_slack_event(&so, false);
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":office: Source Chargeable - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 4);
            assert_eq!(texts[0], "*Source ID:*\n so_001".to_string());
            assert_eq!(texts[1], "*Type:*\n Card".to_string());
            assert_eq!(texts[2], "*Status:*\n chargeable".to_string());
            assert_eq!(texts[3], "*Amount:*\n 12.00 USD".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
    }

    #[test]
    fn test_subscriptionschedule_to_slack_event() {
        let sds = SubscriptionDefaultSettings {
            billing_cycle_anchor: SubscriptionDefaultSettingsBillingCycleAnchor::Automatic,
            collection_method: None,
        };
        let ss = SubscriptionSchedule {
            id: "ss_001".into(),
            customer: "cus_002".into(),
            default_settings: sds,
            end_behavior: SubscriptionScheduleEndBehavior::Cancel,
            event_type: SubscriptionScheduleEventType::SubscriptionScheduleCreated,
            released_at: None,
            canceled_at: None,
            completed_at: None,
            created: 1629000000,
            status: SubscriptionScheduleStatus::Active,
        };
        let ev = subscriptionschedule_to_slack_event(&ss, false);
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, 
                ":incoming_envelope: Subscription Schedule Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }
        
        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 4);
            assert_eq!(texts[0], "*Schedule ID:*\n ss_001".to_string());
            assert_eq!(texts[1], "*Customer:*\n cus_002".to_string());
            assert_eq!(texts[2], "*End Behavior:*\n Cancel".to_string());
            assert_eq!(texts[3], "*Status:*\n Active".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
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
        assert_eq!(ev.blocks.len(), 3);

        if let SlackEventUntagged::Header(ts) = &ev.blocks[0] {
            assert_eq!(ts.text.text, ":top: Topup Created - :ghost: demo :ghost:".to_string());
        } else {
            panic!("Expected first block to be Header");
        }

        if let SlackEventUntagged::Divider(d) = &ev.blocks[1] {
            assert_eq!(d.type_, DividerType::Divider);
        } else {
            panic!("Expected second block to be Divider");
        }

        if let SlackEventUntagged::FieldsSection(fs) = &ev.blocks[2] {
            let texts: Vec<String> = fs.fields.iter().map(|f| f.text.clone()).collect();
            assert_eq!(texts.len(), 3);
            assert_eq!(texts[0], "*Topup ID:*\n tu_001".to_string());
            assert_eq!(texts[1], "*Amount:*\n 60.00 USD".to_string());
            assert_eq!(texts[2], "*Status:*\n Pending".to_string());
        } else {
            panic!("Expected bock to be FieldsSection");
        }
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