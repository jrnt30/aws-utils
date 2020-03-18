package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const batchSize = 10

// This is a small utility to allow migrating an SQS message from one queue to another.
func main() {
	source := flag.String("source", "", "Source queue to read from")
	dest := flag.String("dest", "", "Queue to potentially move data to")
	execute := flag.Bool("execute", false, "Perform migration of the messages to destination queue")
	maxMessageAge := flag.Duration("max-age", time.Hour*12, "Duration of stale messages we are willing to tolerate and republish")
	limit := flag.Int("limit", 10, "Duration of stale messages we are willing to tolerate and republish")
	filter := flag.String("filter", "", "Provides a string filter that can be used to filter the message body")
	verbose := flag.Bool("verbose", false, "Will print additional information for every message to be transmitted")
	flag.Parse()

	var destQueueURL *sqs.GetQueueUrlOutput
	logger := log.New(os.Stdout, "", log.LstdFlags)
	runTime := time.Now()

	if *source == "" {
		logger.Println("Need to provide a source queue name properly to use this utility")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *dest == "" && *execute {
		logger.Println("Need ot provide a destination queue name if attempting to execute a migration")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *execute && *source == *dest {
		logger.Fatal("Need to provide different a different queue name for source and destination")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable}))
	sqsSvc := sqs.New(sess)

	sourceQueueURL, err := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: source})
	if err != nil {
		logger.Println("Encountered an error when attempting to identify the source queue")
		logger.Fatal(err)
	}

	if *dest != "" {
		destQueueURL, err = sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: dest})
		if err != nil {
			logger.Println("Encountered an error when attempting to identify the dest queue")
			logger.Fatal(err)
		}
	}

	logger.Printf("Attempting to load messages less than %s from source queue of %s\n\n", *maxMessageAge, *source)

	count := 0
	for {
		curBatch := batchSize
		left := *limit - count
		if left <= 0 {
			break
		} else if left < batchSize {
			curBatch = left
		}

		messagesToProcess := []*sqs.SendMessageBatchRequestEntry{}
		idsToReceipts := make(map[string]*string)
		queueReceipt, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            sourceQueueURL.QueueUrl,
			AttributeNames:      []*string{aws.String("SentTimestamp")},
			MaxNumberOfMessages: aws.Int64(int64(curBatch)),
			VisibilityTimeout:   aws.Int64(60),
		})
		if err != nil {
			logger.Println("Error encountered when attempting to make a request to get messages")
			logger.Fatal(err)
		}
		if len(queueReceipt.Messages) == 0 {
			break
		}
		for _, message := range queueReceipt.Messages {
			sentTimestamp, _ := strconv.ParseInt(*message.Attributes["SentTimestamp"], 10, 64)
			timeSent := time.Unix(sentTimestamp/1000, 0)
			hoursSince := runTime.Sub(timeSent)
			if hoursSince < *maxMessageAge && strings.Contains(*message.Body, *filter) {
				count++
				logger.Printf("Staging message Age: %s ID: %s Receipt: %s\n", runTime.Sub(timeSent), *message.MessageId, (*message.ReceiptHandle)[:15])
				if *verbose {
					logger.Printf("%s - %s\n", *message.MessageId, *message.Body)
				}
				messagesToProcess = append(messagesToProcess, &sqs.SendMessageBatchRequestEntry{
					Id:          message.MessageId,
					MessageBody: message.Body,
				})
				idsToReceipts[*message.MessageId] = message.ReceiptHandle
			}
		}

		if len(messagesToProcess) > 0 {
			if !*execute {
				logger.Printf("In Dry-Run mode.  This batch would have attempted to process %d messages\n", len(messagesToProcess))
				continue
			}
			resp, err := sqsSvc.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueUrl: destQueueURL.QueueUrl,
				Entries:  messagesToProcess,
			})
			if err != nil {
				logger.Printf("Error attempting to batch migrate messages to SQS")
				logger.Fatal(err)
			}

			for _, failedMigration := range resp.Failed {
				logger.Printf("err with %s - %s", *failedMigration.Id, *failedMigration.Message)
			}

			logger.Println("\nCompleted transfering messages for this batch, resulting in: ")
			logger.Printf("    Successes: %d\n", len(resp.Successful))
			logger.Printf("    Failed: %d\n", len(resp.Failed))

			logger.Println("\nRemoving messages from source queue")
			messagesToDelete := []*sqs.DeleteMessageBatchRequestEntry{}
			for _, successfullyMigrated := range resp.Successful {
				logger.Printf("Staging for removal ID: %s Message ID: %s Receipt: %s\n", *successfullyMigrated.Id, *successfullyMigrated.MessageId, (*idsToReceipts[*successfullyMigrated.Id])[:15])
				messagesToDelete = append(messagesToDelete, &sqs.DeleteMessageBatchRequestEntry{
					Id:            successfullyMigrated.Id,
					ReceiptHandle: idsToReceipts[*successfullyMigrated.Id],
				})
			}
			deletionResp, err := sqsSvc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueUrl: sourceQueueURL.QueueUrl,
				Entries:  messagesToDelete,
			})
			if err != nil {
				logger.Println("Error encountered while attempting to cleanup batch of records")
				logger.Fatal(err)
			}

			logger.Println("\nCompleted removal of messages messages for this batch, resulting in: ")
			logger.Printf("    Successful Removals: %d\n", len(deletionResp.Successful))
			logger.Printf("    Failed Removals: %d\n", len(deletionResp.Failed))
		}
	}
	logger.Printf("Processed %d messages in total", count)
}
