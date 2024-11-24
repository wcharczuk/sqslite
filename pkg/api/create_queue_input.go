package api

type CreateQueueInput struct {
	Attributes map[string]string `type:"map" flattened:"true"`
	QueueName  *string           `type:"string" required:"true"`
	Tags       map[string]string `locationName:"tags" type:"map" flattened:"true"`
}

type CreateQueueOutput struct {
	QueueUrl string `type:"string"`
}
