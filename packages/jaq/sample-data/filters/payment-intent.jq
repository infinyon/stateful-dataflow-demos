if (
  .body.type == "payment_intent.amount_capturable_updated" or
  .body.type == "payment_intent.canceled" or
  .body.type == "payment_intent.created" or
  .body.type == "payment_intent.partially_funded" or
  .body.type == "payment_intent.payment_failed" or
  .body.type == "payment_intent.processing" or
  .body.type == "payment_intent.requires_action" or
  .body.type == "payment_intent.succeeded"
)
then {
  fluvio_version: "0.1",
  api_version: .body.api_version,
  created: .body.created,
  id: .body.id,
  livemode: .body.livemode,
  pending_webhooks: .body.pending_webhooks,
  data: {
    amount: .body.data.object.amount,
    amount_received: .body.data.object.amount_received,
    canceled_at: .body.data.object.canceled_at,
    cancellation_reason: .body.data.object.cancellation_reason,
    capture_method: .body.data.object.capture_method,
    confirmation_method: .body.data.object.confirmation_method,
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    customer: (
      if (.body.data.object.customer | type) == "string" 
      then .body.data.object.customer else "" end
    ),
    description: .body.data.object.description,
    id: .body.data.object.id,
    invoice: (
      if (.body.data.object.invoice | type) == "string" 
      then .body.data.object.invoice else "" end
    ),
    payment_method_types: .body.data.object.payment_method_types,
    receipt_email: .body.data.object.receipt_email,
    status: .body.data.object.status,
    event_type: .body.type
  }
}
else null
end