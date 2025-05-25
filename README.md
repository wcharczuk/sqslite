sqslite
=======

sqslite is designed to be a light-weight, local only testing backend for sqs based queue consumers and producers. 

It's meant to mimic the behaviors of "real" sqs queues, but without the cost or complexity of setting up development only queues.

# Getting started

```bash
> go install github.com/wcharczuk/sqslite@latest
```

# FIFO queues not supported

Fifo Queues are not currently supported. I can add support for them later but we don't use them in practice very often, and I wanted instead to focus on getting standard queues behaving correctly sooner.
