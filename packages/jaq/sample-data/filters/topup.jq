if (
  .body.type == "topup.canceled" or
  .body.type == "topup.created" or
  .body.type == "topup.failed" or
  .body.type == "topup.reversed" or
  .body.type == "topup.succeeded"
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
    description: .body.data.object.description,
    expected_availability_date: .body.data.object.expected_availability_date,
    failure_code: .body.data.object.failure_code,
    failure_message: .body.data.object.failure_message,
    id: .body.data.object.id,
    status: .body.data.object.status,
    event_type: .body.type
  }
}
else null
end