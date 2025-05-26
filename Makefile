run-sketch:
	@ANTHROPIC_API_KEY=$(cat ~/.anthropic_api_key) sketch --skaband-addr=""

run-localstack:
	@LOCALSTACK_SERVICES=s3,sqs DEBUG=1 localstack start
