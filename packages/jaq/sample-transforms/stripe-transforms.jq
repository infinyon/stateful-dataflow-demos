{ 
  fluvio_version: "0.1" 
} +
(
  if (
    .body.type == "invoice.created" or 
    .body.type == "invoice.deleted" or
    .body.type == "invoice.finalized" or
    .body.type == "invoice.finalization_failed" or
    .body.type == "invoice.marked_uncollectible" or
    .body.type == "invoice.payment_action_required" or
    .body.type == "invoice.payment_failed" or
    .body.type == "invoice.payment_succeeded" or
    .body.type == "invoice.paid" or
    .body.type == "invoice.sent" or
    .body.type == "invoice.upcoming" or
    .body.type == "invoice.updated" or 
    .body.type == "invoice.voided" or
    .body.type == "invoice.will_be_due"
  )
  then {
    api_version: .body.api_version,
    created: .body.created,
    id: .body.id,
    livemode: .body.livemode,
    pending_webhooks: .body.pending_webhooks,
    data: (
      {
        account_country: .body.data.object.account_country,
        account_name: .body.data.object.account_name,
        amount_due: .body.data.object.amount_due,
        amount_paid: .body.data.object.amount_paid,
        amount_remaining: .body.data.object.amount_remaining,
        amount_shipping: .body.data.object.amount_shipping,
        attempt_count: .body.data.object.attempt_count,
        attempted: .body.data.object.attempted,
        billing_reason: .body.data.object.billing_reason,
        collection_method: .body.data.object.collection_method,
        created: .body.data.object.created,
        currency: .body.data.object.currency,
        customer: (
          if (.body.data.object.customer | type) == "string" 
          then .body.data.object.customer else "" end
        ),
        customer_email: .body.data.object.customer_email,
        customer_name: .body.data.object.customer_name,
        event_type: .body.type,
        id: .body.data.object.id,
        paid: .body.data.object.paid,
        paid_out_of_band: .body.data.object.paid_out_of_band,
        period_end: .body.data.object.period_end,
        period_start: .body.data.object.period_start,
        status: .body.data.object.status,
        subtotal: .body.data.object.subtotal,
        total: .body.data.object.total
      }
      +
      (
        if .body.data.object.hosted_invoice_url then
          { hosted_invoice_url: .body.data.object.hosted_invoice_url }
        else
          {}
        end
      )
      +
      {
        lines: (.body.data.object.lines.data | map({ description, amount, currency }))
      }
    )
  } 
  elif (
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
  elif (
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
  elif (
    .body.type == "subscription_schedule.aborted" or
    .body.type == "subscription_schedule.canceled" or
    .body.type == "subscription_schedule.completed" or
    .body.type == "subscription_schedule.created" or
    .body.type == "subscription_schedule.expiring" or
    .body.type == "subscription_schedule.released" or
    .body.type == "subscription_schedule.updated"
  )
  then {
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
  elif (
    .body.type == "invoiceitem.created" or
    .body.type == "invoiceitem.deleted"
  )
  then {
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
  elif (
    .body.type == "payment_intent.amount_capturable_updated" or
    .body.type == "payment_intent.canceled" or
    .body.type == "payment_intent.created" or
    .body.type == "payment_intent.partially_funded" or
    .body.type == "payment_intent.payment_failed" or
    .body.type == "payment_intent.processing" or
    .body.type == "payment_intent.requires_action" or
    .body.type == "payment_intent.succeeded"
  )
  then {
    api_version: .body.api_version,
    created: .body.created,
    id: .body.id,
    livemode: .body.livemode,
    pending_webhooks: .body.pending_webhooks,
    data: {
      amount: .body.data.object.amount,
      amount_received: .body.data.object.amount_received,
      canceled_at: .body.data.object.canceled_at,
      cancellation_reason: .body.data.object.cancellation_reason,
      capture_method: .body.data.object.capture_method,
      confirmation_method: .body.data.object.confirmation_method,
      created: .body.data.object.created,
      currency: .body.data.object.currency,
      customer: (
        if (.body.data.object.customer | type) == "string" 
        then .body.data.object.customer else "" end
      ),
      description: .body.data.object.description,
      id: .body.data.object.id,
      invoice: (
        if (.body.data.object.invoice | type) == "string" 
        then .body.data.object.invoice else "" end
      ),
      payment_method_types: .body.data.object.payment_method_types,
      receipt_email: .body.data.object.receipt_email,
      status: .body.data.object.status,
      event_type: .body.type
    }
  }
  elif (
    .body.type == "payout.canceled" or
    .body.type == "payout.created" or
    .body.type == "payout.failed" or
    .body.type == "payout.paid" or
    .body.type == "payout.reconciliation_completed" or
    .body.type == "payout.updated"
  )
  then {
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
  elif (
    .body.type == "issuing_cardholder.created" or
    .body.type == "issuing_cardholder.updated"
  )
  then {
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
      type_: .body.data.object.type,
      event_type: .body.type
    }
  }
  elif (
    .body.type == "issuing_card.created" or
    .body.type == "issuing_card.updated"
  )
  then {
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
      type_: .body.data.object.type,
      event_type: .body.type
    }
  }            
  elif (
    .body.type == "issuing_dispute.closed" or
    .body.type == "issuing_dispute.created" or
    .body.type == "issuing_dispute.funds_reinstated" or
    .body.type == "issuing_dispute.funds_rescinded" or
    .body.type == "issuing_dispute.submitted" or
    .body.type == "issuing_dispute.updated"
  )
  then {
    api_version: .body.api_version,
    created: .body.created,
    id: .body.id,
    livemode: .body.livemode,
    pending_webhooks: .body.pending_webhooks,
    data: {
      amount: .body.data.object.amount,
      created: .body.data.object.created,
      currency: .body.data.object.currency,
      id: .body.data.object.id,
      loss_reason: .body.data.object.loss_reason,
      reason: .body.data.object.evidence.reason,
      status: .body.data.object.status,
      event_type: .body.type
    }
  }
  elif (
    .body.type == "topup.canceled" or
    .body.type == "topup.created" or
    .body.type == "topup.failed" or
    .body.type == "topup.reversed" or
    .body.type == "topup.succeeded"
  )
  then {
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
  elif (
    .body.type == "source.canceled" or
    .body.type == "source.chargeable" or
    .body.type == "source.failed" or
    .body.type == "source.mandate_notification" or
    .body.type == "source.refund_attributes_required" or
    .body.type == "source.transaction.created" or
    .body.type == "source.transaction.updated"
  )
  then {
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
      flow: .body.data.object.flow,
      object: .body.data.object.object,
      event_type: .body.type
    }
  }
  elif (
    .body.type == "issuing_authorization.created" or
    .body.type == "issuing_authorization.updated"
  )
  then {
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
  else {
    api_version: .body.api_version,
    created: .body.created,
    id: .body.id,
    livemode: .body.livemode,
    pending_webhooks: .body.pending_webhooks,
    data: {
      event_type: .body.type,
      message: "Event type not handled"
    }
  }
  end
)