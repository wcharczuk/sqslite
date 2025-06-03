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
	"send-receive":                 sendReceive,
	"send-attribute-md5":           sendAttributeMD5,
	"send-system-attribute-md5":    sendSystemAttributeMD5,
	"fill-dlq":                     fillDLQ,
	"messages-move":                messagesMove,
	"messages-move-invalid-source": messagesMoveInvalidSource,
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
