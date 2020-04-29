package ksink

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"strconv"
	"time"

	blp "github.com/bailaohe/binlog-payload"
	"github.com/bwmarrin/snowflake"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
)

// KafkaSink the sink object of kafka
type KafkaSink struct {
	producer sarama.SyncProducer
	idGen    *snowflake.Node
	recorder KSinkRecorder
	config   *KSinkConfig
}

func (ksink *KafkaSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()
	payload := blp.ParsePayload(e)
	payloadByte, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var id = ksink.idGen.Generate().String()
	var hdrs []sarama.RecordHeader
	var topic = ksink.config.KafkaTopic

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
	if len(ksink.config.TableMapTopic) > 0 {
		for _, m := range ksink.config.TableMapTopic {
			if m.SourceTable == e.Table.Name && m.Topic != "" {
				topic = m.Topic
				break
			}
		}
	}

	var message *sarama.ProducerMessage
	message = &sarama.ProducerMessage{
		Topic:   topic,
		Headers: hdrs,
	}
	message.Value = sarama.StringEncoder(string(payloadByte))
	return []interface{}{
		&message,
	}, nil

}

func (ksink *KafkaSink) Publish(reqs []interface{}) error {

	var logs []*sarama.ProducerMessage
	for _, req := range reqs {
		logs = append(logs, *req.(**sarama.ProducerMessage))
	}

	for _, log := range logs {
		_, _, err := ksink.producer.SendMessage(log)
		if err != nil {
			return errors.Trace(err)
		}
		//logrus.Infof("p: %d, offset: %d", p, o)
	}
	return nil
}

func NewKafkaSink(conf *KSinkConfig, recorder KSinkRecorder) (*KafkaSink, error) {
	var (
		version *sarama.KafkaVersion
		err     error
		sink    KafkaSink
	)
	version, err = parseKafkaVersion(conf.Version)
	if err != nil {
		return nil, err
	}

	cfg := sarama.NewConfig()
	cfg.Version = *version
	// 是否等待成功和失败后的响应,只有的RequireAcks设置不是NoReponse这里才有用
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = selectRequiredAck(2)
	cfg.Producer.Partitioner = selectPartitionerType(conf.PartitionerType)

	if err = cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config invalidate. err: %v", err)
	}

	sink = KafkaSink{
		recorder: recorder,
		config:   conf,
	}

	// 初始化ID生成器
	sink.idGen, err = snowflake.NewNode(conf.ServiceId)
	if err != nil {
		return nil, fmt.Errorf("id gen init err: %+v", err)
	}

	sink.producer, err = sarama.NewSyncProducer(conf.KafkaHosts, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &sink, nil
}

// 选择kafka投递的方式
func selectPartitionerType(partitionerType string) (t sarama.PartitionerConstructor) {
	switch partitionerType {
	case "Manual":
		t = sarama.NewManualPartitioner
	case "RoundRobin":
		t = sarama.NewRoundRobinPartitioner
	case "Random":
		t = sarama.NewRandomPartitioner
	case "Hash":
		t = sarama.NewHashPartitioner
	case "ReferenceHash":
		t = sarama.NewReferenceHashPartitioner
	default:
		t = sarama.NewRoundRobinPartitioner
	}

	return t
}

// 分析kafka版本
func parseKafkaVersion(version string) (*sarama.KafkaVersion, error) {
	var (
		err          error
		kafkaVersion sarama.KafkaVersion
	)

	kafkaVersion, err = sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kafkaVersion, nil
}

// 是否需要确认消息已经正常写入
func selectRequiredAck(ack int) sarama.RequiredAcks {
	var (
		requiredAcks sarama.RequiredAcks
	)
	switch ack {
	case 0:
		requiredAcks = sarama.NoResponse
	case 1:
		requiredAcks = sarama.WaitForLocal
	default:
		requiredAcks = sarama.WaitForAll
	}

	return requiredAcks
}
