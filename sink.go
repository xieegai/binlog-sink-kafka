package ksink

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/bailaohe/binlog-payload"
	"github.com/bwmarrin/snowflake"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"strconv"
)

type KafkaSink struct {
	producer *kafka.Writer
	idGen    *snowflake.Node
	recorder KSinkRecorder
}

func (self *KafkaSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()

	payload := blp.ParsePayload(e)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var id string = self.idGen.Generate().String()

	if self.recorder != nil {
		err = self.recorder.Create(id, payloadBytes)
		if err != nil {
			return nil, err
		}
	}

	id = self.idGen.Generate().String()

	headers := []kafka.Header{
		{"XMEventClass", []byte("com.xiaomai.event.DBSyncEvent")},
		{"XMEventTriggerTime", []byte(strconv.FormatInt(now.Unix(), 10))},
		{"XMEventId", []byte(id)},
	}

	logs := []interface{}{
		&kafka.Message{
			Key:   []byte(id),
			Value: payloadBytes,
			Headers: headers,
		},
	}
	return logs, nil
}

func (self *KafkaSink) Publish(reqs []interface{}) error {

	var logs []kafka.Message
	for _, req := range reqs {
		logs = append(logs, *req.(*kafka.Message))
	}

	err := self.producer.WriteMessages(context.Background(), logs...)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewKafkaSink(conf *KafkaConfig, recorder KSinkRecorder) (*KafkaSink, error) {
	p := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers: strings.Split(conf.KafkaHosts, ","),
			Topic:   conf.KafkaTopic,
		})
	node, err := snowflake.NewNode(conf.ServiceId)
	if err != nil {
		return nil, err
	}
	return &KafkaSink{
		producer: p,
		idGen:    node,
		recorder: recorder,
	}, nil
}