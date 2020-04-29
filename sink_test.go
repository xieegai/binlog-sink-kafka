package ksink

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"strconv"
	"testing"
	"time"
)

type RecorderTest struct {
}

func (r *RecorderTest) Create(id string, payload []byte) error {
	return nil
}

func TestKafkaSink_Publish(t *testing.T) {

	conf := KSinkConfig{
		KafkaHosts:   []string{"xxxxxx:9092"},
		KafkaTopic:   "student_sign_in_fonzie_copy",
		PayloadClass: "com.github.bailaohe.dbsync.event.DBSyncStreamEvent",
		ServiceId:    130,
		Version:      "2.1.0",
		RequiredAcks: 2,
	}
	recorder := RecorderTest{}
	ksink, err := NewKafkaSink(&conf, &recorder)
	if err != nil {
		logrus.Fatalf("初始化kafka客户端 err: %+v", err)
	}

	for i := 0; i < 100; i++ {
		var id = ksink.idGen.Generate().String()
		var hdrs []sarama.RecordHeader
		now := time.Now()

		hdrs = []sarama.RecordHeader{
			{
				Key:   []byte("XMEventClass"),
				Value: []byte("ksink.config.PayloadClass"),
			},
			{
				Key:   []byte("XMEventTriggerTime"),
				Value: []byte(strconv.FormatInt(now.Unix(), 10)),
			},
			{
				Key:   []byte("XMEventId"),
				Value: []byte(id),
			},
		}

		var message *sarama.ProducerMessage
		message = &sarama.ProducerMessage{
			Topic:   ksink.config.KafkaTopic,
			Headers: hdrs,
		}

		message.Value = sarama.StringEncoder(fmt.Sprintf(`{"message": %d}`, i))
		err := ksink.Publish([]interface{}{&message})
		if err != nil {
			logrus.Fatalf("kafka消息投递失败 err : %v", err)
		}
		logrus.Infof("message: %d", i)
		time.Sleep(1 * time.Second)
	}
}
