package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	queueName      = flag.String("queue-name", "test-queue-name", "Queue name")
	timeoutSeconds = flag.Int64("timeout-seconds", 20, "(Optional) Timeout in seconds for long polling")
)

func main() {

	flag.Parse()

	if *queueName == "" {
		flag.PrintDefaults()
		exitErrorf("Queue name required")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(*queueName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			exitErrorf("Unable to find queue %q.", *queueName)
		}
		exitErrorf("Unable to queue %q, %v.", *queueName, err)
	}

	for x := 0; x < 100; x++ {
		result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: resultURL.QueueUrl,
			AttributeNames: aws.StringSlice([]string{
				"SentTimestamp",
			}),
			MaxNumberOfMessages: aws.Int64(1),
			MessageAttributeNames: aws.StringSlice([]string{
				"All",
			}),
			WaitTimeSeconds: aws.Int64(*timeoutSeconds),
		})
		if err != nil {
			exitErrorf("Unable to receive message from queue %q, %v.", *queueName, err)
		}
		fmt.Printf("Received %d messages.\n", len(result.Messages))
		if len(result.Messages) > 0 {
			fmt.Println(result.Messages)
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
