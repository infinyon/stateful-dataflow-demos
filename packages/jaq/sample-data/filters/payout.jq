if (
  .body.type == "payout.canceled" or
  .body.type == "payout.created" or
  .body.type == "payout.failed" or
  .body.type == "payout.paid" or
  .body.type == "payout.reconciliation_completed" or
  .body.type == "payout.updated"
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
    arrival_date: .body.data.object.arrival_date,
    automatic: .body.data.object.automatic,
    balance_transaction: (
      if (.body.data.object.balance_transaction | type) == "string" 
      then .body.data.object.balance_transaction else "" end
    ),
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    description: .body.data.object.description,
    failure_code: .body.data.object.failure_code,
    failure_message: .body.data.object.failure_message,
    id: .body.data.object.id,
    method: .body.data.object.method,
    reconciliation_status: .body.data.object.reconciliation_status,
    source_type: .body.data.object.source_type,
    statement_descriptor: .body.data.object.statement_descriptor,
    status: .body.data.object.status,
    type: .body.data.object.type,
    event_type: .body.type
  }
}
else null
end