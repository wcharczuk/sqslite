sqslite
=======

sqslite is designed to be a light-weight, local only testing backend for sqs based queue consumers and producers. 

It's meant to mimic the behaviors of "real" sqs queues, but without the cost or complexity of setting up development only queues.

# Getting started

```bash
> go install github.com/wcharczuk/sqslite@latest
```

# Limitations

This goes without saying but this server is not strictly perfectly at parity with the live AWS SQS service for things like
- error messages strictly
- legacy api calling conventions / formats outside json
- authentication
- kms / encryption 

As well FIFO Queues are not currently supported. I can add support for them later but we don't use them in practice very often, and I wanted instead to focus on getting standard queues behaving correctly sooner.
