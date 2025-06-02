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

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"

	"github.com/wcharczuk/sqslite/internal/integration"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

var (
	flagAWSRegion  = pflag.String("region", sqslite.DefaultRegion, "The AWS region")
	flagLocal      = pflag.Bool("local", false, "If we should target a local sqslite instance")
	flagMode       = pflag.String("mode", string(integration.ModeVerify), "The integration test mode (save|verify)")
	flagOutputPath = pflag.String("output-path", "testdata/integration", "The output path in --mode=save, and the source path in --mode=verify")
	flagScenarios  = pflag.StringSlice("scenario", nil, fmt.Sprintf(
		"The integration test scenarios to run (%s)",
		strings.Join(slices.Collect(maps.Keys(scenarios)), "|"),
	))
)

// assertions
// - ✅ can recreate queues with the same attributes => yields the same queue url
// - ✅ what is the missing required parameter error type
// - ✅ what error is returned by sendMessage if the body is > 256KiB
// - ✅ sendMessageBatch requires the sum of all the bodes to be < 256KiB
// - ✅ startMessageMoveTask what happens if you put a MaxNumberOfMessagesPerSecond > 500
// - startMessageMoveTask what happens if you put a source that isn't a dlq
// - startMessageMoveTask what happens if you put a destination that disallows a given source with a redriveAllowPolicy
// - startMessageMoveTask what happens if you delete the destination queue with a huge backlog

func main() {
	pflag.Parse()
	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	it := integration.Suite{
		Region:     *flagAWSRegion,
		Local:      *flagLocal,
		Mode:       integration.Mode(*flagMode),
		OutputPath: *flagOutputPath,
	}

	var enabledScenarios []string
	if len(*flagScenarios) == 0 {
		enabledScenarios = slices.Collect(maps.Keys(scenarios))
	} else {
		enabledScenarios = *flagScenarios
	}
	for _, scenario := range enabledScenarios {
		fn, ok := scenarios[scenario]
		if !ok {
			continue
		}
		slog.Info("integration scenario starting", slog.String("scenario", scenario))
		if err := it.Run(ctx, scenario, fn); err != nil {
			slog.Error("integration scenario failed", slog.String("scenario", scenario), slog.Any("err", err))
			os.Exit(1)
		}
		slog.Info("integration scenario completed successfully", slog.String("scenario", scenario))
	}
}

var scenarios = map[string]func(*integration.Run){
	"send-receive":                 sendReceive,
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

	queueAttributes := it.GetQueueAttributes(dlq, types.QueueAttributeNameApproximateNumberOfMessages)
	if value := queueAttributes["ApproximateNumberOfMessages"]; value != "5" {
		panic(fmt.Errorf("expected dlq to have 5 messages, has %s", value))
	}

	taskHandle := it.StartMessagesMoveTask(dlq, mainQueue)

done:
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
		for _, t := range tasks {
			if t.Status == "COMPLETED" {
				break done
			}
			if t.Status == "FAILED" {
				panic(t.FailureReason)
			}
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
