package ksink

// KSinkConfig the configuration of kafka sink in MySQL sync
type KSinkConfig struct {
	// the host of kafka cluster to publish sync
	KafkaHosts []string
	// the kafka topic to hold the rows change
	KafkaTopic string

	// the payload class path used by java consumer
	PayloadClass string

	// the service id to facilitate the snowflake id generation
	ServiceId int64

	// kafka version
	Version string

	// 消息投递方式
	PartitionerType string

	// 是否需要确认消息已经正常写入
	RequiredAcks  int
	TableMapTopic []TableMapTopic
}

type TableMapTopic struct {
	SourceTable string
	Topic       string
}
