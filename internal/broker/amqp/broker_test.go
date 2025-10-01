package amqp

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/surendratiwari3/paota/config"
	"testing"
)

// func TestNewAMQPBroker(t *testing.T) {
// 	// This test is commented out due to missing mock interfaces
// 	// Integration tests should be used instead of unit tests for this functionality
// }

func TestAMQPBrokerGetRoutingKey(t *testing.T) {
	cfg := &config.Config{
		Broker:        "amqp",
		TaskQueueName: "test_queue",
		AMQP: &config.AMQPConfig{
			Exchange:           "exchange",
			ExchangeType:       "fanout",
			BindingKey:         "test_key",
			Url:                "amqp://localhost:5672",
			HeartBeatInterval:  30,
			ConnectionPoolSize: 2,
			DelayedQueue:       "delay_queue",
			PrefetchCount:      100,
		},
	}

	broker := &AMQPBroker{
		config: cfg,
	}

	require.Equal(t, "test_queue", broker.getTaskQueue(), "TaskQueueName should match")
	require.Equal(t, "test_queue", broker.getRoutingKey(), "Routing key should match the direct exchange binding key")
	require.Equal(t, "fanout", broker.getExchangeType(), "exchange name key should match")
	require.Equal(t, "delay_queue", broker.getDelayedQueue(), "delay_queue name key should match")
	require.Equal(t, 100, broker.getQueuePrefetchCount(), "prefetch count key should match")
	require.Equal(t, "exchange", broker.getExchangeName(), "Exchange key should match")
	require.Equal(t, "exchange", broker.getDelayedQueueDLX(), "Exchange key should match")
}

func TestIsDirectExchange(t *testing.T) {
	// Prepare a sample AMQPBroker instance with a direct exchange
	cfg := &config.Config{
		AMQP: &config.AMQPConfig{
			ExchangeType: "direct",
		},
	}
	amqpBroker := &AMQPBroker{
		config: cfg,
	}

	// Check if the exchange type is direct
	isDirect := amqpBroker.isDirectExchange()
	if !isDirect {
		t.Error("Expected exchange type to be direct, got false")
	}

	// Prepare another sample AMQPBroker instance with a different exchange type
	cfgNonDirect := &config.Config{
		AMQP: &config.AMQPConfig{
			ExchangeType: "fanout",
		},
	}
	amqpBrokerNonDirect := &AMQPBroker{
		config: cfgNonDirect,
	}

	// Check if the exchange type is not direct
	isDirectNonDirect := amqpBrokerNonDirect.isDirectExchange()
	if isDirectNonDirect {
		t.Error("Expected exchange type not to be direct, got true")
	}
}

func TestIsConnectionError(t *testing.T) {
	broker := &AMQPBroker{}

	// Test a few specific connection error patterns
	testCases := []struct {
		errMsg   string
		shouldBe bool
	}{
		{"connection closed", true},
		{"connection lost", true},
		{"channel closed", true},
		{"broken pipe", true},
		{"connection refused", true},
		{"timeout", true},
		{"EOF", true},
		{"use of closed network connection", true},
		{"connection pool is empty", true},
		{"connection pool is invalid", true},
		{"invalid message format", false},
		{"queue not found", false},
		{"permission denied", false},
		{"authentication failed", false},
		{"validation error", false},
	}

	for _, tc := range testCases {
		err := errors.New(tc.errMsg)
		result := broker.isConnectionError(err)
		assert.Equal(t, tc.shouldBe, result, "Error message: %s, expected: %v, got: %v", tc.errMsg, tc.shouldBe, result)
	}

	// Test nil error
	assert.False(t, broker.isConnectionError(nil), "Should not detect connection error for nil")
}

func TestCheckConnectionHealthWithNilConnection(t *testing.T) {
	broker := &AMQPBroker{
		connection: nil,
	}

	err := broker.CheckConnectionHealth()
	assert.Error(t, err, "Health check should fail with nil connection")
	assert.Contains(t, err.Error(), "AMQP connection is nil")
}
