if (
  .body.type == "invoice.created" or 
  .body.type == "invoice.deleted" or
  .body.type == "invoice.finalized" or
  .body.type == "invoice.finalization_failed" or
  .body.type == "invoice.marked_uncollectible" or
  .body.type == "invoice.payment_action_required" or
  .body.type == "invoice.payment_failed" or
  .body.type == "invoice.payment_succeeded" or
  .body.type == "invoice.paid" or
  .body.type == "invoice.sent" or
  .body.type == "invoice.upcoming" or
  .body.type == "invoice.updated" or 
  .body.type == "invoice.voided" or
  .body.type == "invoice.will_be_due"
)
then {
  fluvio_version: "0.1",
  api_version: .body.api_version,
  created: .body.created,
  id: .body.id,
  livemode: .body.livemode,
  pending_webhooks: .body.pending_webhooks,
  data: (
    {
      account_country: .body.data.object.account_country,
      account_name: .body.data.object.account_name,
      amount_due: .body.data.object.amount_due,
      amount_paid: .body.data.object.amount_paid,
      amount_remaining: .body.data.object.amount_remaining,
      amount_shipping: .body.data.object.amount_shipping,
      attempt_count: .body.data.object.attempt_count,
      attempted: .body.data.object.attempted,
      billing_reason: .body.data.object.billing_reason,
      collection_method: .body.data.object.collection_method,
      created: .body.data.object.created,
      currency: .body.data.object.currency,
      customer: (
        if (.body.data.object.customer | type) == "string" 
        then .body.data.object.customer else "" end
      ),
      customer_email: .body.data.object.customer_email,
      customer_name: .body.data.object.customer_name,
      event_type: .body.type,
      id: .body.data.object.id,
      paid: .body.data.object.paid,
      paid_out_of_band: .body.data.object.paid_out_of_band,
      period_end: .body.data.object.period_end,
      period_start: .body.data.object.period_start,
      status: .body.data.object.status,
      subtotal: .body.data.object.subtotal,
      total: .body.data.object.total
    }
    +
    (
      if .body.data.object.hosted_invoice_url then
        { hosted_invoice_url: .body.data.object.hosted_invoice_url }
      else
        {}
      end
    )
    +
    {
      lines: (.body.data.object.lines.data | map({ description, amount, currency }))
    }
  )
} 
else null
end