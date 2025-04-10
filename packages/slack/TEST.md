### Test Slack

```bash
curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' https://hooks.slack.com/services/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
```

```bash
curl -X POST -H 'Content-type: application/json' --data '{"blocks":[{"text":{"text":"New *Stripe* event - *InvoiceEventType::InvoiceCreated* (Some(InvoiceStatus::Draft)) :memo:","type":"mrkdwn"},"type":"section"},{"fields":[{"text":"*Account:*\nInfinyOn (US)","type":"mrkdwn"},{"text":"*Customer:*\nNick Cardin <nick+fc9@infinyon.com>","type":"mrkdwn"},{"text":"*Amount Due:*\n0.00 USD","type":"mrkdwn"},{"text":"*Amount Paid:*\n0.00 USD","type":"mrkdwn"},{"text":"*Period:*\nApr 03, 2025 - Apr 03, 2025","type":"mrkdwn"},{"text":"*Items:*\n-","type":"mrkdwn"}],"type":"section"}]}' https://hooks.slack.com/services/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
```