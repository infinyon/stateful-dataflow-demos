if (
  .body.type == "issuing_authorization.created" or
  .body.type == "issuing_authorization.updated"
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
    amount_details: .body.data.object.amount_details,
    approved: .body.data.object.approved,
    authorization_method: .body.data.object.authorization_method,
    card: (.body.data.object.card.id // null),
    cardholder: (.body.data.object.cardholder // null),
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    id: .body.data.object.id,
    merchant_amount: .body.data.object.merchant_amount,
    merchant_currency: .body.data.object.merchant_currency,
    merchant_data: .body.data.object.merchant_data,
    status: .body.data.object.status,
    wallet: .body.data.object.wallet,
    event_type: .body.type
  }
}
else null
end