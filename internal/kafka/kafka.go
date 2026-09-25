// Package kafka centralizes topic names and constructors for Kafka
// (Redpanda) readers and writers, so every service agrees on names and
// balancing strategy. We use segmentio/kafka-go, a pure-Go client.
package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Topic names. Keeping them here prevents typos across services.
const (
	// TopicGPSPings carries every raw ping. Partitioned by vehicle_id so all
	// pings for one vehicle are ordered and land on the same partition/consumer.
	TopicGPSPings = "gps-pings"
	// TopicGeofenceAlerts carries geofence entry/exit violations.
	TopicGeofenceAlerts = "geofence-alerts"
	// TopicSpeedAlerts carries speeding events.
	TopicSpeedAlerts = "speed-alerts"
)

// NewWriter builds a writer that keys messages by vehicle_id. The Hash balancer
// maps a message key deterministically to a partition, which is what guarantees
// per-vehicle ordering. RequireOne waits for the leader to ack (a balance of
// durability and latency).
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		// We do our own application-level batching in the ingestion service, but
		// these bounds also let kafka-go coalesce writes efficiently.
		BatchSize:    500,
		BatchTimeout: 100 * time.Millisecond,
		Async:        false,
	}
}

// NewReader builds a consumer-group reader. All readers sharing groupID split
// the topic's partitions between them (horizontal scaling); Kafka tracks each
// group's committed offset so a restart resumes where it left off.
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB: wait for a little data to reduce round-trips
		MaxBytes:       10e6, // 10MB max per fetch
		CommitInterval: time.Second,
	})
}
