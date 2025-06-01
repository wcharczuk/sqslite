sqslite
=======

[![Continuous Integration](https://github.com/wcharczuk/sqslite/actions/workflows/ci.yaml/badge.svg)](https://github.com/wcharczuk/sqslite/actions/workflows/ci.yaml)


[sqslite](https://github.com/wcharczuk/sqslite) is designed to be a lightweight, local development only testing backend for SQS based queue consumers and producers. 

It's meant to mimic the behaviors of the "real" sqs service, but without the cost or complexity of setting up real SQS.

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

This goes without saying but this server is not strictly perfectly at parity with the live AWS SQS service for things like
- error messages / codes
- legacy api calling conventions / formats outside json
- authentication (outside parsing the request account ID)
- kms / encryption of message bodies

As well FIFO Queues are not currently supported. Support can be added for them later but we don't use them in practice very often, and wanted instead to focus on getting standard queues behaving correctly.
