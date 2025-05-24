
run-localstack:
	@LOCALSTACK_SERVICES=s3,sqs DEBUG=1 localstack start
