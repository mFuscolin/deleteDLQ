package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	svc      *sqs.SQS
	queueURL = "YOUR_DEAD_LETTER_QUEUE_NAME"
)

func handleMessage(msg *sqs.Message) {
	fmt.Println("RECEIVING MESSAGE >>> ")
	fmt.Println(*msg.Body)
}

func deleteMessage(msg *sqs.Message) {

	svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
}

// UnixTimeStampToTime
func UnixTimeStampToTime(unixTimeStamp string) time.Time {

	timestamp, err := strconv.ParseInt(unixTimeStamp, 10, 64)

	if err != nil {
		panic(err)
	}

	return time.Unix(0, timestamp*int64(time.Millisecond))

}

// extractValue Extracts a specific key from the body of the message
func extractValue(body string, key string) string {
	keystr := "\"" + key + "\":[^,;\\]}]*"
	r, _ := regexp.Compile(keystr)
	match := r.FindString(body)
	keyValMatch := strings.Split(match, ":")
	return strings.ReplaceAll(keyValMatch[1], "\"", "")
}

func receiveSQSMessages(ctx context.Context, svc *sqs.SQS) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Receive up to 10 messages from SQS FIFO queue
			result, err := svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: aws.Int64(10),
				WaitTimeSeconds:     aws.Int64(20),
				AttributeNames: []*string{
					aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
					aws.String("All"),
				},
			})
			if err != nil {
				fmt.Println("Error receiving messages:", err)
				return
			}

			// Process each message individually
			for _, msg := range result.Messages {
				str := extractValue(string(*msg.Body), "CodMsg")
				convertedTime := UnixTimeStampToTime(*msg.Attributes["SentTimestamp"])
				if str == "specify_body_value" {
					handleMessage(msg)
					fmt.Println("DATA:", convertedTime.String())
					deleteMessage(msg)
				}
			}

			// Exit the loop when there are no more messages
			if len(result.Messages) == 0 {
				return
			}
		}
	}
}

func main() {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("your_aws_queue_region"),
		Credentials: credentials.NewEnvCredentials(),
	})

	if err != nil {
		panic(err)
	}

	svc = sqs.New(sess)

	// Create a context for the SQS message receiver
	ctx, cancel := context.WithCancel(context.Background())

	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start receiving messages from SQS FIFO queue
	go receiveSQSMessages(ctx, svc)

	// Wait for OS signal to gracefully shutdown the SQS message receiver
	<-sigChan
	fmt.Println("Shutting down...")
	cancel()
}
