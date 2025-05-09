if (
  .body.type == "subscription_schedule.aborted" or
  .body.type == "subscription_schedule.canceled" or
  .body.type == "subscription_schedule.completed" or
  .body.type == "subscription_schedule.created" or
  .body.type == "subscription_schedule.expiring" or
  .body.type == "subscription_schedule.released" or
  .body.type == "subscription_schedule.updated"
)
then {
  fluvio_version: "0.1",
  api_version: .body.api_version,
  created: .body.created,
  id: .body.id,
  livemode: .body.livemode,
  pending_webhooks: .body.pending_webhooks,
  data: {
    canceled_at: .body.data.object.canceled_at,
    completed_at: .body.data.object.completed_at,
    created: .body.data.object.created,
    customer: (
      if (.body.data.object.customer | type) == "string" 
      then .body.data.object.customer else "" end
    ),
    default_settings: {
      billing_cycle_anchor: .body.data.object.default_settings.billing_cycle_anchor,
      collection_method: .body.data.object.default_settings.collection_method
    },
    end_behavior: .body.data.object.end_behavior,
    id: .body.data.object.id,
    released_at: .body.data.object.released_at,
    status: .body.data.object.status,
    event_type: .body.type
  }
}
else null
end