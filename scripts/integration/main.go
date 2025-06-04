package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"

	"github.com/wcharczuk/sqslite/internal/integration"
	"github.com/wcharczuk/sqslite/internal/slant"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

var (
	flagLogLevel   = pflag.String("log-level", slog.LevelInfo.String(), "The logger level")
	flagLogFormat  = pflag.String("log-format", "json", "The logger format (json|text)")
	flagAWSRegion  = pflag.String("region", sqslite.DefaultRegion, "The AWS region")
	flagLocal      = pflag.Bool("local", false, "If we should target a locally running instance for the upstream")
	flagMode       = pflag.String("mode", string(integration.ModeVerify), "The integration test mode (save|verify)")
	flagOutputPath = pflag.String("output-path", "testdata/integration", "The output path in --mode=save, and the source path in --mode=verify")
	flagScenarios  = pflag.StringSlice("scenario", nil, fmt.Sprintf(
		"The integration test scenarios to run (%s)",
		strings.Join(slices.Collect(maps.Keys(scenarios)), "|"),
	))
)

func main() {
	pflag.Parse()
	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	logLeveler := new(slog.LevelVar)
	if err := logLeveler.UnmarshalText([]byte(*flagLogLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level: %v\n", err)
		os.Exit(1)
	}
	switch *flagLogFormat {
	case "json":
		slog.SetDefault(
			slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
	case "text":
		slog.SetDefault(
			slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
		slant.Print(os.Stdout, "sqslite verifier")
	default:
		slog.Error("invalid log level", slog.String("log_level", *flagLogLevel))
		os.Exit(1)
	}
	slog.Error("using log level", slog.String("log_level", logLeveler.Level().String()))

	it := integration.Suite{
		Region:     *flagAWSRegion,
		Mode:       integration.Mode(*flagMode),
		OutputPath: *flagOutputPath,
		Local:      *flagLocal,
	}

	var enabledScenarios []string
	if len(*flagScenarios) == 0 {
		enabledScenarios = slices.Collect(maps.Keys(scenarios))
	} else {
		enabledScenarios = *flagScenarios
	}
	if len(enabledScenarios) == 0 {
		slog.Error("must provide at least (1) integration test scenario")
		os.Exit(1)
	}
	for _, scenario := range enabledScenarios {
		fn, ok := scenarios[scenario]
		if !ok {
			continue
		}
		slog.Info("integration scenario starting", slog.String("scenario", scenario), slog.String("mode", *flagMode))
		if err := it.Run(ctx, scenario, fn); err != nil {
			slog.Error("integration scenario failed", slog.String("scenario", scenario), slog.Any("err", err), slog.String("mode", *flagMode))
			os.Exit(1)
		}
		slog.Info("integration scenario completed successfully", slog.String("scenario", scenario), slog.String("mode", *flagMode))
	}
}

var scenarios = map[string]func(*integration.Run){
	"send-body-raw-text":                     sendBodyRawText,
	"send-body-invalid-json":                 sendBodyInvalidJSON,
	"send-body-invalid-string":               sendBodyInvalidString,
	"send-receive":                           sendReceive,
	"send-attribute-md5":                     sendAttributeMD5,
	"send-system-attribute-md5":              sendSystemAttributeMD5,
	"receive-message-order":                  receiveMessageOrder,
	"receive-attribute-names":                receiveAttributeNames,
	"receive-attribute-names-single":         receiveAttributeNamesSingle,
	"receive-attribute-names-all":            receiveAttributeNamesAll,
	"receive-attribute-names-none":           receiveAttributeNamesNone,
	"receive-system-attribute-names-all":     receiveSystemAttributeNamesAll,
	"receive-system-attribute-names-subset":  receiveSystemAttributeNamesSubset,
	"receive-system-attribute-names-invalid": receiveSystemAttributeNamesInvalid,
	"fill-dlq":                               fillDLQ,
	"messages-move":                          messagesMove,
	"messages-move-invalid-source":           messagesMoveInvalidSource,
}

func sendBodyRawText(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)
	it.SendMessageWithBody(mainQueue, "this is a test")
	receiptHandle, ok := it.ReceiveMessage(mainQueue)
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func sendBodyInvalidJSON(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)
	it.SendMessageWithBody(mainQueue, `{"message_index`)
	receiptHandle, ok := it.ReceiveMessage(mainQueue)
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func sendBodyInvalidString(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)
	it.ExpectFailure(func() {
		it.SendMessageWithBody(mainQueue, string([]byte{0xC0, 0xAF, 0xFE, 0xFF}))
	})
}

func sendReceive(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}
	for range 5 {
		receiptHandle, ok := it.ReceiveMessage(mainQueue)
		if ok {
			it.DeleteMessage(mainQueue, receiptHandle)
		}
	}
}

func sendAttributeMD5(it *integration.Run) {
	mainQueue := it.CreateQueue()

	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
	})
	receiptHandle, ok := it.ReceiveMessage(mainQueue)
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func sendSystemAttributeMD5(it *integration.Run) {
	mainQueue := it.CreateQueue()

	it.SendMessageWithSystemAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
	}, map[string]types.MessageSystemAttributeValue{
		"AWSTraceHeader": {
			DataType:    aws.String("String"),
			StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
		},
	})
	receiptHandle, ok := it.ReceiveMessage(mainQueue)
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveMessageOrder(it *integration.Run) {
	mainQueue := it.CreateQueue()
	for range 100 {
		it.SendMessage(mainQueue)
	}
	var remaining = 100
	for {
		handles := it.ReceiveMessages(mainQueue)
		remaining = remaining - len(handles)
		if remaining == 0 {
			break
		}
	}
}

func receiveAttributeNames(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithAttributeNames(mainQueue, []string{"test-key", "test-key-alt"})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveAttributeNamesSingle(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithAttributeNames(mainQueue, []string{"test-key"})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveAttributeNamesAll(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithAttributeNames(mainQueue, []string{"All"})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveAttributeNamesNone(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithAttributeNames(mainQueue, nil)
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveSystemAttributeNamesAll(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithSystemAttributeNames(mainQueue, nil, []types.MessageSystemAttributeName{
		types.MessageSystemAttributeNameAll,
	})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveSystemAttributeNamesSubset(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithSystemAttributeNames(mainQueue, nil, []types.MessageSystemAttributeName{
		types.MessageSystemAttributeNameSentTimestamp,
		types.MessageSystemAttributeNameApproximateReceiveCount,
		types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp,
		types.MessageSystemAttributeNameDeadLetterQueueSourceArn,
	})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func receiveSystemAttributeNamesInvalid(it *integration.Run) {
	mainQueue := it.CreateQueue()
	it.SendMessageWithAttributes(mainQueue, map[string]types.MessageAttributeValue{
		"test-key": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value"),
		},
		"test-key-alt": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test-value-alt"),
		},
	})
	receiptHandle, ok := it.ReceiveMessageWithSystemAttributeNames(mainQueue, nil, []types.MessageSystemAttributeName{
		types.MessageSystemAttributeName("not-a-real-name"),
	})
	if ok {
		it.DeleteMessage(mainQueue, receiptHandle)
	}
}

func fillDLQ(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}
	for range integration.RedrivePolicyMaxReceiveCount {
		for range 5 {
			receiptHandle, ok := it.ReceiveMessage(mainQueue)
			if ok {
				it.ChangeMessageVisibility(mainQueue, receiptHandle, 0)
			}
		}
	}
	it.Sleep(time.Second)
}

func messagesMove(it *integration.Run) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}

	for range integration.RedrivePolicyMaxReceiveCount {
		for range 5 {
			receiptHandle, ok := it.ReceiveMessage(mainQueue)
			if ok {
				it.ChangeMessageVisibility(mainQueue, receiptHandle, 0)
			}
		}
	}
	it.Sleep(time.Second)

	queueAttributes := it.GetQueueAttributes(dlq, types.QueueAttributeNameApproximateNumberOfMessages)
	if value := queueAttributes["ApproximateNumberOfMessages"]; value != "5" {
		panic(fmt.Errorf("expected dlq to have 5 messages, has %s", value))
	}

	taskHandle := it.StartMessagesMoveTask(dlq, mainQueue)

	for {
		tasks := it.ListMessagesMoveTasks(dlq)
		if len(tasks) == 0 {
			panic("expect at least one task")
		}
		if !slices.ContainsFunc(tasks, func(t integration.MoveMessagesTask) bool {
			return t.Status != "RUNNING" || (t.Status == "RUNNING" && t.TaskHandle == taskHandle)
		}) {
			panic("expect at least one task to have the correct task handle")
		}
		if slices.ContainsFunc(tasks, func(t integration.MoveMessagesTask) bool {
			return t.Status == "FAILED"
		}) {
			panic("messages move task has FAILED status")
		}
		if slices.ContainsFunc(tasks, func(t integration.MoveMessagesTask) bool {
			return t.Status == "COMPLETED"
		}) {
			break
		}
		it.Sleep(time.Second)
	}

	var messageReciptHandles []string
	for range 5 {
		messageReciptHandle, ok := it.ReceiveMessage(mainQueue)
		if !ok {
			continue
		}
		messageReciptHandles = append(messageReciptHandles, messageReciptHandle)
	}
	if len(messageReciptHandles) != 5 {
		panic("expect final moved message count to be 5")
	}
	for _, msg := range messageReciptHandles {
		it.DeleteMessage(mainQueue, msg)
	}
}

func messagesMoveInvalidSource(it *integration.Run) {
	notDLQ := it.CreateQueue()
	mainQueue := it.CreateQueue()

	it.ExpectFailure(func() {
		_ = it.StartMessagesMoveTask(notDLQ, mainQueue)
	})
}
