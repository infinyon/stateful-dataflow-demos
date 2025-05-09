if (
  .body.type == "charge.captured" or
  .body.type == "charge.dispute.closed" or
  .body.type == "charge.dispute.created" or
  .body.type == "charge.dispute.funds_reinstated" or
  .body.type == "charge.dispute.funds_withdrawn" or
  .body.type == "charge.dispute.updated" or
  .body.type == "charge.expired" or
  .body.type == "charge.failed" or
  .body.type == "charge.pending" or
  .body.type == "charge.refund.updated" or
  .body.type == "charge.refunded" or
  .body.type == "charge.succeeded" or
  .body.type == "charge.updated"
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
    amount_captured: .body.data.object.amount_captured,
    amount_refunded: .body.data.object.amount_refunded,
    balance_transaction: (
      if (.body.data.object.balance_transaction | type) == "string" 
      then .body.data.object.balance_transaction else "" end
    ),
    calculated_statement_descriptor: .body.data.object.calculated_statement_descriptor,
    captured: .body.data.object.captured,
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    customer: (
      if (.body.data.object.customer | type) == "string" 
      then .body.data.object.customer else "" end
    ),
    description: .body.data.object.description,
    disputed: .body.data.object.disputed,
    failure_code: .body.data.object.failure_code,
    failure_message: .body.data.object.failure_message,
    id: .body.data.object.id,
    invoice: (
      if (.body.data.object.invoice | type) == "string" 
      then .body.data.object.invoice else "" end
    ),        
    paid: .body.data.object.paid,
    receipt_url: .body.data.object.receipt_url,
    refunded: .body.data.object.refunded,
    status: .body.data.object.status,
    event_type: .body.type
  }
}
else null
end