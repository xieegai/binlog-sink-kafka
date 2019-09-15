package ksink

type KafkaConfig struct {
	KafkaHosts string
	KafkaTopic string

	ServiceId  int64



	//EnableRecorder bool
	//RecorderAddr   string
	//RecorderDB     string
}
