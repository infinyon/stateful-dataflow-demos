if (
  .body.type == "issuing_card.created" or
  .body.type == "issuing_card.updated"
)
then {
  fluvio_version: "0.1",
  api_version: .body.api_version,
  created: .body.created,
  id: .body.id,
  livemode: .body.livemode,
  pending_webhooks: .body.pending_webhooks,
  data: {
    brand: .body.data.object.brand,
    cancellation_reason: .body.data.object.cancellation_reason,
    cardholder: {
      email: .body.data.object.cardholder.email,
      id: .body.data.object.cardholder.id
    },
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    cvc: .body.data.object.cvc,
    exp_month: .body.data.object.exp_month,
    exp_year: .body.data.object.exp_year,
    financial_account: .body.data.object.financial_account,
    id: .body.data.object.id,
    last4: .body.data.object.last4,
    status: .body.data.object.status,
    type: .body.data.object.type,
    event_type: .body.type
  }
}
else null
end