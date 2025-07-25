package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/urfave/cli/v3"

	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func main() {
	if err := root.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

var root = &cli.Command{
	Name:  "sqsctl",
	Usage: "Control sqslite servers",
	Commands: []*cli.Command{
		sendMessage,
		queues,
	},
}

var debugEnabled bool

var defaultFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:        "debug",
		Destination: &debugEnabled,
	},
	&cli.StringFlag{
		Name:  "region",
		Usage: "The aws region to configure the client with",
		Value: "us-west-2",
	},
	&cli.StringFlag{
		Name:  "endpoint",
		Usage: "The endpoint to configure the client with",
		Value: "http://localhost:4566",
	},
	&cli.StringFlag{
		Name:  "account-id",
		Usage: "The account ID to query with",
		Value: sqslite.DefaultAccountID,
	},
}

func debugf(format string, args ...any) {
	if debugEnabled {
		fmt.Fprintln(os.Stderr, "[DEBUG] "+fmt.Sprintf(format, args...))
	}
}

var sendMessage = &cli.Command{
	Name:    "send-message",
	Aliases: []string{"send"},
	Usage:   "Send a message to a given queue",
	Flags: append(defaultFlags,
		&cli.StringFlag{
			Name:  "queue-url",
			Usage: "The Queue URL to target",
		},
		&cli.StringFlag{
			Name:  "id",
			Usage: "The sqs send message id",
			Value: uuid.V4().String(),
		},
		&cli.DurationFlag{
			Name:  "delay",
			Usage: "The sqs send message delay",
		},
		&cli.StringMapFlag{
			Name:  "attribute",
			Usage: "message attributes",
		},
	),
	Action: func(ctx context.Context, c *cli.Command) error {
		contents, err := fileOrStdin(c.Args().First())
		if err != nil {
			return err
		}
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})
		var delaySeconds int32
		if delay := c.Duration("delay"); delay > 0 {
			delaySeconds = int32(delay / time.Second)
		}

		queueURL := resolveQueueURL(c)
		output, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:          aws.String(queueURL),
			MessageBody:       aws.String(string(contents)),
			DelaySeconds:      delaySeconds,
			MessageAttributes: formatMessageAttributes(c.StringMap("attribute")),
		})
		if err != nil {
			return err
		}
		fmt.Println(*output.MessageId)
		return nil
	},
}

var queues = &cli.Command{
	Name:    "queues",
	Aliases: []string{"queue"},
	Usage:   "Control sqslite queues",
	Commands: []*cli.Command{
		queueCreate,
		queueUpdate,
		queuesList,
		queueDescribe,
		queueDelete,
	},
}

var queueCreate = &cli.Command{
	Name:  "create",
	Usage: "Create a queue",
	Flags: append(defaultFlags,
		&cli.StringFlag{
			Name:  "name",
			Usage: "The sqs queue name",
			Value: uuid.V4().String(),
		},
		&cli.StringMapFlag{
			Name:  "attribute",
			Usage: "The sqs queue attributes",
		},
		&cli.StringMapFlag{
			Name:  "tag",
			Usage: "The sqs queue tags",
		},
	),
	Action: func(ctx context.Context, c *cli.Command) error {
		debugf("creating queue %s", c.String("name"))
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})
		res, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName:  aws.String(c.String("name")),
			Attributes: c.StringMap("attribute"),
			Tags:       c.StringMap("tags"),
		})
		if err != nil {
			return err
		}
		fmt.Println(*res.QueueUrl)
		return nil
	},
}

var queueUpdate = &cli.Command{
	Name:  "update",
	Usage: "Update a queue",
	Flags: append(defaultFlags,
		&cli.StringFlag{
			Name:  "queue-url",
			Usage: "The Queue URL to target",
		},
		&cli.StringMapFlag{
			Name:  "attribute",
			Usage: "The sqs queue attributes",
		},
		&cli.StringMapFlag{
			Name:  "tag",
			Usage: "The sqs queue tags",
		},
	),
	Action: func(ctx context.Context, c *cli.Command) error {
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})
		_, err = sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl:   aws.String(resolveQueueURL(c)),
			Attributes: c.StringMap("attribute"),
		})
		if err != nil {
			return err
		}
		_, err = sqsClient.TagQueue(ctx, &sqs.TagQueueInput{
			QueueUrl: aws.String(c.String("queue-url")),
			Tags:     c.StringMap("tag"),
		})
		if err != nil {
			return err
		}
		return nil
	},
}

var queuesList = &cli.Command{
	Name:  "list",
	Usage: "List queues",
	Flags: defaultFlags,
	Action: func(ctx context.Context, c *cli.Command) error {
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})

		res, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{
			MaxResults: aws.Int32(100),
		})
		if err != nil {
			return err
		}
		for _, queueURL := range res.QueueUrls {
			fmt.Println(queueURL)
		}
		for res.NextToken != nil {
			res, err = sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{
				NextToken:  res.NextToken,
				MaxResults: aws.Int32(100),
			})
			if err != nil {
				return err
			}
			for _, queueURL := range res.QueueUrls {
				fmt.Println(queueURL)
			}
		}
		return nil
	},
}

var queueDescribe = &cli.Command{
	Name:  "describe",
	Usage: "Describe a queue",
	Flags: append(defaultFlags,
		&cli.StringFlag{
			Name:  "queue-url",
			Usage: "The Queue URL to target",
		},
	),
	Action: func(ctx context.Context, c *cli.Command) error {
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})
		res, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(resolveQueueURL(c)),
			AttributeNames: types.QueueAttributeNameAll.Values(),
		})
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "\t")
		enc.Encode(res.Attributes)
		return nil
	},
}

var queueDelete = &cli.Command{
	Name:  "delete",
	Usage: "Delete a queue",
	Flags: append(defaultFlags,
		&cli.StringFlag{
			Name:  "queue-url",
			Usage: "The Queue URL to target",
		},
	),
	Action: func(ctx context.Context, c *cli.Command) error {
		sess, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.String("region")),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.String("account-id"), "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			return err
		}
		sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(c.String("endpoint"))
			o.AppID = "sqslite-demo-producer"
		})
		_, err = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: aws.String(resolveQueueURL(c)),
		})
		if err != nil {
			return err
		}
		fmt.Printf("deleted queue %s\n", c.String("queue-url"))
		return nil
	},
}

func resolveQueueURL(c *cli.Command) string {
	if queueURL := c.String("queue-url"); queueURL != "" {
		return queueURL
	}
	if accountID := c.String("account-id"); accountID != "" {
		return sqslite.FormatQueueURL(sqslite.Authorization{
			AccountID: accountID,
		}, sqslite.DefaultQueueName)
	}
	return sqslite.FormatQueueURL(sqslite.DefaultAuthorization, sqslite.DefaultQueueName)
}

func formatMessageAttributes(attributes map[string]string) map[string]types.MessageAttributeValue {
	output := make(map[string]types.MessageAttributeValue, len(attributes))
	for key, value := range attributes {
		output[key] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value),
		}
	}
	return output
}

func fileOrStdin(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}
