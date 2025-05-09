if (
  .body.type == "customer.updated" or
  .body.type == "customer.bank_account.created" or
  .body.type == "customer.bank_account.deleted" or
  .body.type == "customer.bank_account.updated" or
  .body.type == "customer.card.created" or
  .body.type == "customer.card.deleted" or
  .body.type == "customer.card.updated" or
  .body.type == "customer.created" or
  .body.type == "customer.deleted" or
  .body.type == "customer.subscription.created" or
  .body.type == "customer.subscription.deleted" or
  .body.type == "customer.subscription.paused" or
  .body.type == "customer.subscription.pending_update_applied" or
  .body.type == "customer.subscription.pending_update_expired" or
  .body.type == "customer.subscription.resumed" or
  .body.type == "customer.subscription.trial_will_end" or
  .body.type == "customer.subscription.updated"
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
      balance: .body.data.object.balance,
      created: .body.data.object.created,
      currency: .body.data.object.currency,
      delinquent: .body.data.object.delinquent,
      description: .body.data.object.description,
      email: .body.data.object.email,
      event_type: .body.type,
      id: .body.data.object.id,
      invoice_prefix: .body.data.object.invoice_prefix,
      name: .body.data.object.name,
      next_invoice_sequence: .body.data.object.next_invoice_sequence,
      phone: .body.data.object.phone
    }
    +
    (
      if .body.data.object.address then
        {
          address: {
            city: .body.data.object.address.city,
            country: .body.data.object.address.country,
            line1: .body.data.object.address.line1,
            line2: .body.data.object.address.line2,
            "postal-code": .body.data.object.address["postal-code"],
            state: .body.data.object.address.state
          }
        }
      else
        {}
      end
    )
  )
}
else null
end