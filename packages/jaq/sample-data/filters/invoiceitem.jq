if (
  .body.type == "invoiceitem.created" or
  .body.type == "invoiceitem.deleted"
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
    currency: .body.data.object.currency,
    customer: (
      if (.body.data.object.customer | type) == "string" 
      then .body.data.object.customer else "" end
    ),
    date: .body.data.object.date,
    description: .body.data.object.description,
    id: .body.data.object.id,
    period: { 
      start: .body.data.object.period.start, 
      end: .body.data.object.period.end 
    },
    quantity: .body.data.object.quantity,
    event_type: .body.type
  }
}
else null
end