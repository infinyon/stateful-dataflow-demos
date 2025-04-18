dependencies:
- name: chrono
  version: "0.4.39"
run: |
fn stripe_to_slack(se: StripeEvent) -> Result<SlackObj> {
  use chrono::{DateTime, Utc, TimeZone};
  
  match &se.data {
    EventData::Invoice(invoice) => {            
      // Convert timestamp to humanly readable date
      let default_datetime: DateTime<Utc> = Utc
          .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
          .single()
          .unwrap_or_else(|| Utc::now());
      let period_start: DateTime<Utc> = Utc
          .timestamp_opt(invoice.period_start as i64, 0)
          .single()
          .unwrap_or(default_datetime);
      let period_end: DateTime<Utc> = Utc
          .timestamp_opt(invoice.period_end as i64, 0)
          .single()
          .unwrap_or(default_datetime);
      let formatted_start = period_start.format("%b %d, %Y").to_string();
      let formatted_end = period_end.format("%b %d, %Y").to_string();

      // Format the amounts (assuming the amounts are in cents).
      let amount_due = invoice.amount_due as f64 / 100.0;
      let amount_paid = invoice.amount_paid as f64 / 100.0;

      // Format invoice items. If there are no items, display a hyphen.
      let items_text = if invoice.lines.is_empty() {
          "-".to_string()
      } else {
          invoice.lines
              .iter()
              .map(|line| {
                  // Convert each line item's amount from cents to dollars.
                  let line_amount = line.amount as f64 / 100.0;
                  format!("- {} (${:.2} {})", line.description, line_amount, line.currency)
              })
              .collect::<Vec<_>>()
              .join("\n")
      };

      // Only add the memo emoji if livemode is false.
      let memo_suffix = if !se.livemode { " :memo:" } else { "" };

      // Save title 
      let title = format!("New *Stripe* event - *{:?}* ({:?}){}",
          invoice.event_type, invoice.status, memo_suffix);

      // Save fields
      let mut fields: Vec<String> = vec![];
      fields.push(
        format!("*Account:*\n{} ({})", 
          invoice.account_name.clone().unwrap_or("".to_string()), 
          invoice.account_country.clone().unwrap_or("".to_string())
        )
      );
      fields.push(
        format!("*Customer:*\n{} <{}>", 
          invoice.customer_name.clone().unwrap_or("".to_string()), 
          invoice.customer_email.clone().unwrap_or("".to_string())
        )
      );
      fields.push(format!("*Amount Due:*\n{:.2} USD", amount_due));
      fields.push(format!("*Period:*\n{} - {}", formatted_start, formatted_end));
      fields.push(format!("*Items:*\n{}", items_text));

      // Generate fields object
      let fields_obj = SlackObj {
        title: title,
        fields: fields
      };

      Ok(fields_obj)
    }
    _ => Err(sdfg::anyhow::anyhow!("Unsupported stripe event"))
  }
}