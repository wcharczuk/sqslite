sqslite
=======

[![Continuous Integration](https://github.com/wcharczuk/sqslite/actions/workflows/ci.yaml/badge.svg)](https://github.com/wcharczuk/sqslite/actions/workflows/ci.yaml)


[sqslite](https://github.com/wcharczuk/sqslite) is designed to be a lightweight, local development only testing backend for SQS based queue consumers and producers. 

It's meant to mimic the behaviors of the "real" sqs service, but without the cost or complexity of setting up real SQS, specifically such that you can develop services safely against sqslite and be confident that they will then behave correctly in production.

Critically it:
- simulates SQS queue sharding; messages come back in semi-random order, and rarely come back in full batches
- enforces all of the validations and constraints that normal queues impose; e.g. 256KiB size limits, valid utf-8 chars etc.
- enforces lifecycle constraints like 60s cooloffs on queue deletion
- (2025-07-27) support for "fair queues" has been added

# Getting started

```bash
> go install github.com/wcharczuk/sqslite@latest
```

You can then run the server with:

```
> sqslite --bind-addr=":4566"
```

This will start the server while listening on port `4566`, feel free to change this to whatever you need.

To then connect with this server, you will need to change how you instantiate your sqs clients (this assumes you're using [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)):

```go
sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
	o.BaseEndpoint = "http://localhost:4566"
})
```

# Limitations

FIFO queues are not supported. 

As well, any features to do with Policies (i.e. IAM) and KMS are not supported.
