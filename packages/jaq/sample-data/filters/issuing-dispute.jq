if (
  .body.type == "issuing_dispute.closed" or
  .body.type == "issuing_dispute.created" or
  .body.type == "issuing_dispute.funds_reinstated" or
  .body.type == "issuing_dispute.funds_rescinded" or
  .body.type == "issuing_dispute.submitted" or
  .body.type == "issuing_dispute.updated"
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
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    id: .body.data.object.id,
    loss_reason: .body.data.object.loss_reason,
    reason: .body.data.object.evidence.reason,
    status: .body.data.object.status,
    event_type: .body.type
  }
}
else null
end