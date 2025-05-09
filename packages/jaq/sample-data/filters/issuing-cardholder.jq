if (
  .body.type == "issuing_cardholder.created" or
  .body.type == "issuing_cardholder.updated"
)
then {
  fluvio_version: "0.1",
  api_version: .body.api_version,
  created: .body.created,
  id: .body.id,
  livemode: .body.livemode,
  pending_webhooks: .body.pending_webhooks,
  data: {
    billing: {
      city: .body.data.object.billing.address.city,
      country: .body.data.object.billing.address.country,
      line1: .body.data.object.billing.address.line1,
      line2: .body.data.object.billing.address.line2,
      postal_code: .body.data.object.billing.address.postal_code,
      state: .body.data.object.billing.address.state
    },
    created: .body.data.object.created,
    email: .body.data.object.email,
    id: .body.data.object.id,
    individual: {
      dob: {
        day: .body.data.object.individual.dob.day,
        month: .body.data.object.individual.dob.month,
        year: .body.data.object.individual.dob.year
      },
      first_name: .body.data.object.individual.first_name,
      last_name: .body.data.object.individual.last_name
    },
    name: .body.data.object.name,
    phone_number: .body.data.object.phone_number,
    status: .body.data.object.status,
    type: .body.data.object.type,
    event_type: .body.type
  }
}
else null
end