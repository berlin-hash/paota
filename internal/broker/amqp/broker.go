package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/surendratiwari3/paota/config"
	"github.com/surendratiwari3/paota/internal/broker"
	"github.com/surendratiwari3/paota/internal/provider"
	"github.com/surendratiwari3/paota/internal/workergroup"
	"github.com/surendratiwari3/paota/logger"
	"github.com/surendratiwari3/paota/schema"
	"github.com/surendratiwari3/paota/schema/errors"
	"strings"
	"sync"
	"time"
)

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	config           *config.Config
	ackChannel       chan uint64
	connectionsMutex sync.Mutex
	processingWG     sync.WaitGroup
	amqpErrorChannel <-chan *amqp.Error
	stopChannel      chan struct{}
	doneStopChannel  chan struct{}
	amqpProvider     provider.AmqpProviderInterface
	connection       *amqp.Connection
}

// globalAmqpProvider defined just for unit test cases
var (
	globalAmqpProvider provider.AmqpProviderInterface
)

func (b *AMQPBroker) getConnection() (*amqp.Connection, error) {
	amqpConn, err := b.amqpProvider.GetConnectionFromPool()
	if err != nil {
		return nil, err
	}
	if amqpConnection, ok := amqpConn.(*amqp.Connection); ok {
		return amqpConnection, nil
	}
	return nil, nil
}

// getRoutingKey gets the routing key from the signature
func (b *AMQPBroker) getRoutingKey() string {
	if b.config.AMQP.BindingKey == "" {
		return b.getTaskQueue()
	}
	if b.isDirectExchange() {
		return b.config.AMQP.BindingKey
	}

	return b.getTaskQueue()
}

// isDirectExchange checks if the exchange type is direct
func (b *AMQPBroker) isDirectExchange() bool {
	return b.config.AMQP != nil && b.config.AMQP.ExchangeType == "direct"
}

// NewAMQPBroker creates a new instance of the AMQP broker
// It opens connections to RabbitMQ, declares an exchange, opens a channel,
// declares and binds the queue, and enables publish notifications
func NewAMQPBroker(configProvider config.ConfigProvider) (broker.Broker, error) {
	cfg := configProvider.GetConfig()
	amqpErrorChannel := make(chan *amqp.Error, 1)
	stopChannel := make(chan struct{})
	doneStopChannel := make(chan struct{})
	amqpBroker := &AMQPBroker{
		config:           cfg,
		connectionsMutex: sync.Mutex{},
		amqpErrorChannel: amqpErrorChannel,
		stopChannel:      stopChannel,
		doneStopChannel:  doneStopChannel,
		amqpProvider:     globalAmqpProvider,
	}

	if amqpBroker.amqpProvider == nil {
		amqpBroker.amqpProvider = provider.NewAmqpProvider(cfg.AMQP)
	}

	err := amqpBroker.amqpProvider.CreateConnectionPool()
	if err != nil {
		logger.ApplicationLogger.Error("failed to created connection pool, return", err)
		return nil, err
	}
	if cfg.AMQP.CrashOnConnectionError {
		// Get a connection for error monitoring
		conn, err := amqpBroker.getConnection()
		if err != nil {
			logger.ApplicationLogger.Error("failed to get connection for error monitoring, return", err)
			return nil, err
		}
		amqpBroker.connection = conn

		// Set up error channel monitoring
		amqpBroker.amqpErrorChannel = conn.NotifyClose(make(chan *amqp.Error, 1))
	}

	// Set up exchange, queue, and binding
	if err := amqpBroker.setupExchangeQueueBinding(); err != nil {
		logger.ApplicationLogger.Error("failed to created exchange queue binding, return", err)
		return nil, err
	}

	return amqpBroker, nil
}

func (b *AMQPBroker) processDelivery(ctx context.Context, amqpMsg amqp.Delivery, workerGroup workergroup.WorkerGroupInterface) error {
	if len(amqpMsg.Body) == 0 {
		logger.ApplicationLogger.Error("empty message, return")
		err := amqpMsg.Nack(false, false)
		if err != nil {
			return err
		} // multiple, requeue
		return errors.ErrEmptyMessage // RabbitMQ down?
	}

	workerGroup.AssignJob(amqpMsg)
	b.processingWG.Done()
	return nil
}

// Publish sends a task to the AMQP broker.
// If the Signature contains RawArgs, it publishes only RawArgs as the message body,
// and encodes metadata in AMQP headers. Otherwise, it publishes the entire Signature as JSON.
func (b *AMQPBroker) Publish(ctx context.Context, signature *schema.Signature) error {
	body, headers, err := b.prepareMessageBodyAndHeaders(signature)
	if err != nil {
		return err
	}

	amqpPublishMessage := amqp.Publishing{
		ContentType:  "application/json",
		Priority:     signature.Priority,
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
	}

	delayMs := b.getTaskTTL(signature)
	if delayMs > 0 {
		amqpPublishMessage.Expiration = fmt.Sprint(delayMs)
	} else if signature.RoutingKey == "" {
		signature.RoutingKey = b.getRoutingKey()
	}

	if !b.shouldCrashOnConnectionErrors() {
		return b.amqpProvider.AmqpPublishWithConfirm(ctx, signature.RoutingKey, amqpPublishMessage, b.getExchangeName())
	} else {
		err = b.amqpProvider.AmqpPublishWithConfirm(ctx, signature.RoutingKey, amqpPublishMessage, b.getExchangeName())
		if err != nil {
			logger.ApplicationLogger.Error("failed to publish message to AMQP, connection may be lost", err)
			// Check if this is a connection error and crash the worker
			if b.isConnectionError(err) {
				if b.shouldCrashOnConnectionErrors() {
					logger.ApplicationLogger.Error("AMQP connection error detected during publish, crashing worker", err)
					panic(fmt.Sprintf("AMQP connection lost during publish: %v", err))
				}
				// If not crashing, just return error to caller
			}
			return err
		}

		return nil
	}

}

// prepareMessageBodyAndHeaders returns the AMQP message body and headers.
// If RawArgs is set on the Signature, RawArgs is used as the body and other metadata
// is encoded into headers. Otherwise, the entire Signature is marshaled into the body.
func (b *AMQPBroker) prepareMessageBodyAndHeaders(signature *schema.Signature) ([]byte, amqp.Table, error) {
	var body []byte
	var err error
	headers := amqp.Table{}

	if len(signature.RawArgs) > 0 && string(signature.RawArgs) != "null" {
		body = signature.RawArgs
		headers = buildHeadersFromSignature(signature)
	} else {
		body, err = json.Marshal(signature)
		if err != nil {
			return nil, nil, fmt.Errorf("JSON marshal error: %w", err)
		}
	}

	return body, headers, nil
}

// buildHeadersFromSignature converts task metadata fields from the Signature
// into AMQP headers. Used when publishing RawArgs-only messages.
func buildHeadersFromSignature(sig *schema.Signature) amqp.Table {
	headers := amqp.Table{
		"uuid":                   sig.UUID,
		"task_name":              sig.Name,
		"retry_count":            sig.RetryCount,
		"retry_timeout":          sig.RetryTimeout,
		"timeout":                sig.TaskTimeOut,
		"wait_time":              sig.WaitTime,
		"retries_done":           sig.RetriesDone,
		"ignore_if_unregistered": sig.IgnoreWhenTaskNotRegistered,
	}

	if sig.CreatedAt != nil {
		headers["created_at"] = sig.CreatedAt.Unix()
	}
	if sig.ETA != nil {
		headers["eta"] = sig.ETA.Unix()
	}

	return headers
}

// setupExchangeQueueBinding sets up the exchange, queue, and binding
func (b *AMQPBroker) setupExchangeQueueBinding() error {
	amqpConn, err := b.getConnection()
	if err != nil {
		return err
	}
	defer func(amqpProvider provider.AmqpProviderInterface, i interface{}) {
		err := b.amqpProvider.ReleaseConnectionToPool(i)
		if err != nil {
			//TODO:error handling
		}
	}(b.amqpProvider, amqpConn)

	channel, _, err := b.amqpProvider.CreateAmqpChannel(amqpConn, false)
	if err != nil {
		return err
	}
	defer func(channel *amqp.Channel) {
		err := b.amqpProvider.CloseAmqpChannel(channel)
		if err != nil {
			//TODO: error handling
		}
	}(channel)

	// Declare the exchange
	err = b.amqpProvider.DeclareExchange(channel, b.getExchangeName(), b.getExchangeType())
	if err != nil {
		return err
	}

	declareQueueArgs := amqp.Table(b.config.AMQP.QueueDeclareArgs)

	// Declare the task queue
	err = b.amqpProvider.DeclareQueue(channel, b.getTaskQueue(), declareQueueArgs)
	if err != nil {
		return err
	}

	// Declare the delay queue
	declareQueueArgs = amqp.Table{
		// Exchange where to send messages after TTL expiration.
		"x-dead-letter-exchange": b.getDelayedQueueDLX(),
		// Routing key which use when resending expired messages.
		"x-dead-letter-routing-key": b.getRoutingKey(),
	}
	err = b.amqpProvider.DeclareQueue(channel, b.getDelayedQueue(), declareQueueArgs)
	if err != nil {
		return err
	}
	// Bind the queue to the exchange
	err = b.amqpProvider.QueueExchangeBind(channel, b.getTaskQueue(), b.config.AMQP.BindingKey, b.config.AMQP.Exchange)
	if err != nil {
		return err
	}

	err = b.amqpProvider.QueueExchangeBind(channel, b.getDelayedQueue(), b.getDelayedQueue(), b.getDelayedQueueDLX())
	if err != nil {
		return err
	}

	if fq := b.getFailedQueue(); fq != "" {
		declareFailedQueueArgs := amqp.Table(b.config.AMQP.QueueDeclareArgs)
		// Bind Queue and Bind
		err = b.amqpProvider.DeclareQueue(channel, fq, declareFailedQueueArgs)
		if err != nil {
			return err
		}
		err = b.amqpProvider.QueueExchangeBind(channel, fq, fq, b.config.AMQP.Exchange)
		if err != nil {
			return err
		}
	}

	if tq := b.getTimeoutQueue(); tq != "" {
		declareTimeoutQueueArgs := amqp.Table(b.config.AMQP.QueueDeclareArgs)
		// Bind Timeout Queue and Bind
		err = b.amqpProvider.DeclareQueue(channel, tq, declareTimeoutQueueArgs)
		if err != nil {
			return err
		}
		err = b.amqpProvider.QueueExchangeBind(channel, tq, tq, b.config.AMQP.Exchange)
		if err != nil {
			return err
		}
	}

	return nil
}

// StopConsumer stops the AMQP consumer
func (b *AMQPBroker) StopConsumer() {
	b.stopChannel <- struct{}{}
	<-b.doneStopChannel
}

// StartConsumer initializes the AMQP consumer
func (b *AMQPBroker) StartConsumer(ctx context.Context, workerGroup workergroup.WorkerGroupInterface) error {
	queueName := b.getTaskQueue()

	conn, err := b.getConnection()
	if err != nil {
		logger.ApplicationLogger.Error("failed to get connection for consumer", err)
		if b.shouldCrashOnConnectionErrors() {
			panic(fmt.Sprintf("AMQP connection failed: %v", err))
		}
		return err
	}
	defer func(amqpProvider provider.AmqpProviderInterface, i interface{}) {
		err := b.amqpProvider.ReleaseConnectionToPool(i)
		if err != nil {
			//TODO:error handling
		}
	}(b.amqpProvider, conn)

	// Create a channel
	channel, _, err := b.amqpProvider.CreateAmqpChannel(conn, false)
	if err != nil {
		logger.ApplicationLogger.Error("failed to create AMQP channel", err)
		if b.shouldCrashOnConnectionErrors() {
			panic(fmt.Sprintf("AMQP channel creation failed: %v", err))
		}
		return err
	}
	defer func(channel *amqp.Channel) {
		err := b.amqpProvider.CloseAmqpChannel(channel)
		if err != nil {
			logger.ApplicationLogger.Error("failed to start consumer, exit", err)
			//TODO:error handling
		}
	}(channel)

	// Channel QOS
	if err = b.amqpProvider.SetChannelQoS(channel, b.getQueuePrefetchCount()); err != nil {
		logger.ApplicationLogger.Error("failed to set channel qos", err)
		if b.shouldCrashOnConnectionErrors() {
			panic(fmt.Sprintf("AMQP channel QoS setup failed: %v", err))
		}
		return err
	}

	deliveries, err := b.amqpProvider.CreateConsumer(channel, queueName, workerGroup.GetWorkerGroupName())
	if err != nil {
		logger.ApplicationLogger.Error("failed to create consumer", err)
		if b.shouldCrashOnConnectionErrors() {
			panic(fmt.Sprintf("AMQP consumer creation failed: %v", err))
		}
		return err
	}

	logger.ApplicationLogger.Info("[*] Waiting for messages. To exit press CTRL+C")

	// Monitor both the main connection error channel and the consumer connection
	errorsChan := make(chan error, 1)
	amqpErrorChannel := b.amqpErrorChannel

	// Start a goroutine to monitor the consumer connection for errors
	if b.shouldCrashOnConnectionErrors() {
		go func() {
			// Monitor channel close notifications
			channelCloseChan := channel.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case channelErr := <-channelCloseChan:
				if channelErr != nil {
					logger.ApplicationLogger.Error("AMQP channel closed unexpectedly", channelErr)
					errorsChan <- fmt.Errorf("AMQP channel closed: %v", channelErr)
				}
			case <-ctx.Done():
				return
			}
		}()
	}

	for {
		select {
		case amqpErr := <-amqpErrorChannel:
			logger.ApplicationLogger.Error("AMQP connection lost", amqpErr)
			if b.shouldCrashOnConnectionErrors() {
				panic(fmt.Sprintf("AMQP connection lost: %v", amqpErr))
			}
			return amqpErr
		case err := <-errorsChan:
			logger.ApplicationLogger.Error("AMQP error detected", err)
			if b.shouldCrashOnConnectionErrors() {
				panic(fmt.Sprintf("AMQP error: %v", err))
			}
			return err
		case d := <-deliveries:
			b.processingWG.Add(1)
			err := b.processDelivery(ctx, d, workerGroup)
			if err != nil {
				logger.ApplicationLogger.Warning("delivery in error, continue")
			}
		case <-b.stopChannel:
			b.doneStopChannel <- struct{}{}
			logger.ApplicationLogger.Warning("stop request in consumer, exit")
			if b.shouldCrashOnConnectionErrors() {
				b.processingWG.Wait()
			}
			return nil
		}
	}
	b.processingWG.Wait()
	return nil
}

func (b *AMQPBroker) getDelayedQueue() string {
	return b.config.AMQP.DelayedQueue
}

func (b *AMQPBroker) getFailedQueue() string {
	return b.config.AMQP.FailedQueue
}

func (b *AMQPBroker) getTimeoutQueue() string {
	return b.config.AMQP.TimeoutQueue
}

func (b *AMQPBroker) getQueuePrefetchCount() int {
	return b.config.AMQP.PrefetchCount
}

func (b *AMQPBroker) getDelayedQueueDLX() string {
	return b.config.AMQP.Exchange
}

func (b *AMQPBroker) getExchangeName() string {
	return b.config.AMQP.Exchange
}

func (b *AMQPBroker) getExchangeType() string {
	return b.config.AMQP.ExchangeType
}

func (b *AMQPBroker) getTaskQueue() string {
	return b.config.TaskQueueName
}

func (b *AMQPBroker) getTaskTTL(task *schema.Signature) int64 {
	// If ETA is not defined, no delay
	if task.ETA == nil {
		return 0
	}

	// If ETA is defined, check priority + TaskTimeOut + CreatedAt
	if task.Priority > 1 && task.TaskTimeOut != 0 && task.CreatedAt != nil {
		return 5 // send 5 milliseconds
	}

	// Otherwise calculate delay based on ETA
	now := time.Now().UTC()
	if task.ETA.After(now) {
		return int64(task.ETA.Sub(now) / time.Millisecond)
	}

	return 0
}

// isConnectionError checks if the error is related to AMQP connection issues
func (b *AMQPBroker) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common AMQP connection error patterns
	connectionErrorPatterns := []string{
		"connection closed",
		"connection lost",
		"channel closed",
		"broken pipe",
		"connection refused",
		"timeout",
		"eof",
		"use of closed network connection",
		"connection pool is empty",
		"connection pool is invalid",
	}

	for _, pattern := range connectionErrorPatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}

	return false
}

// shouldCrashOnConnectionErrors returns whether the consumer should crash on AMQP errors
func (b *AMQPBroker) shouldCrashOnConnectionErrors() bool {
	if b == nil || b.config == nil || b.config.AMQP == nil {
		return true
	}
	return b.config.AMQP.CrashOnConnectionError
}

// CheckConnectionHealth verifies if the AMQP connection is still healthy
// This method can be called periodically to monitor connection health
func (b *AMQPBroker) CheckConnectionHealth() error {
	if b.connection == nil {
		return fmt.Errorf("AMQP connection is nil")
	}

	// Try to get a connection from the pool to verify it's working
	conn, err := b.getConnection()
	if err != nil {
		logger.ApplicationLogger.Error("connection health check failed, connection lost", err)
		return fmt.Errorf("connection health check failed: %v", err)
	}

	// Release the connection back to the pool
	defer func() {
		if releaseErr := b.amqpProvider.ReleaseConnectionToPool(conn); releaseErr != nil {
			logger.ApplicationLogger.Error("failed to release connection during health check", releaseErr)
		}
	}()

	// Try to create a channel to verify the connection is working
	channel, _, err := b.amqpProvider.CreateAmqpChannel(conn, false)
	if err != nil {
		logger.ApplicationLogger.Error("connection health check failed, cannot create channel", err)
		return fmt.Errorf("connection health check failed, cannot create channel: %v", err)
	}

	// Close the test channel
	if closeErr := b.amqpProvider.CloseAmqpChannel(channel); closeErr != nil {
		logger.ApplicationLogger.Error("failed to close test channel during health check", closeErr)
	}

	return nil
}
