package ksink

// KSinkConfig the configuration of kafka sink in MySQL sync
type KSinkConfig struct {
	// the host of kafka cluster to publish sync
	KafkaHosts string
	// the kafka topic to hold the rows change
	KafkaTopic string

	// the service id to facilitate the snowflake id generation
	ServiceId  int64
}
