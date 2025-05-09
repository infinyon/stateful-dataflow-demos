if (
  .body.type == "source.canceled" or
  .body.type == "source.chargeable" or
  .body.type == "source.failed" or
  .body.type == "source.mandate_notification" or
  .body.type == "source.refund_attributes_required" or
  .body.type == "source.transaction.created" or
  .body.type == "source.transaction.updated"
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
    client_secret: .body.data.object.client_secret,
    created: .body.data.object.created,
    currency: .body.data.object.currency,
    customer: .body.data.object.customer,
    id: .body.data.object.id,
    owner: (
      if .body.data.object.owner then {
        address: (
          if .body.data.object.owner.address then {
            city: .body.data.object.owner.address.city,
            country: .body.data.object.owner.address.country,
            line1: .body.data.object.owner.address.line1,
            line2: .body.data.object.owner.address.line2,
            postal_code: .body.data.object.owner.address.postal_code,
            state: .body.data.object.owner.address.state
          } else {} end
        ),
        email: .body.data.object.owner.email,
        name: .body.data.object.owner.name,
        phone: .body.data.object.owner.phone
      } else {} end
    ),
    statement_descriptor: .body.data.object.statement_descriptor,
    status: .body.data.object.status,
    type: .body.data.object.type,
    event_type: .body.type
  }
}
else null
end