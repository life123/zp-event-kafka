package zp_event_kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"github.com/ziipin-server/niuhe"
)

type ZPEventKafkaProducer struct {
	Topic         string
	KafkaProducer sarama.AsyncProducer
}

func NewKafkaAsyncProducer(addrs []string, topic string) *ZPEventKafkaProducer {
	if len(addrs) == 0 || topic == "" {
		niuhe.LogError("NewKafkaAsyncProducer parameters invalid")
		panic("NewKafkaAsyncProducer parameters invalid")
	}

	metrics.UseNilMetrics = true //禁用掉统计
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(
		addrs,
		kafkaConfig,
	)
	if nil != err {
		niuhe.LogError("[NewKafkaAsyncProducer]init kafka producer fail, %v", err)
		panic("NewKafkaAsyncProducer init kafka producer fail")
	}
	zpEventKafkaProducer := &ZPEventKafkaProducer{
		Topic:         topic,
		KafkaProducer: producer,
	}
	return zpEventKafkaProducer
}

func (self *ZPEventKafkaProducer) PushDataToKafka(data interface{}) {
	var msg *sarama.ProducerMessage

	dataByte, _ := json.Marshal(data)

	msg = &sarama.ProducerMessage{
		Topic: self.Topic,
		Key:   sarama.StringEncoder(self.Topic),
		Value: sarama.ByteEncoder(dataByte),
	}
	self.KafkaProducer.Input() <- msg
	select {
	case _ = <-self.KafkaProducer.Successes():
		//niuhe.LogInfo("[PushDataToKafka] success, offset:%d, t:v", succ.Offset, succ.Timestamp)
	case fail := <-self.KafkaProducer.Errors():
		niuhe.LogError("[PushDataToKafka] err:%v", fail.Err)
	}
}

type ZPEvent struct {
	ZpId      string `json:"zp_id"` // 统一设备标识号，必填
	Timestamp int64  `json:"t"`     // 事件发生时间戳，精确到毫秒
	//Type       string            `json:"type"`   // 用户行为统一填 track，
	EventName  string            `json:"et"`         // 事件名，由后台统一分配，不由业务部门内部自己取值
	AppID      string            `json:"app_id"`     // 应用标识
	Ip         string            `json:"ip"`         // 用户行为发生时的客户端IP地址，尽可能提供
	Addr       string            `json:"addr"`       // IP对应的地址，尽可能提供，为空的话，预处理插件在数据入Hadoop之前会通过IP查一次地址，补全这个字段
	AppUser    string            `json:"app_user"`   // 应用内账户标识，已登录用户必填
	ZpUser     string            `json:"zp_user"`    // 通行证id，通行证登录用户必填
	Properties map[string]string `json:"properties"` // 事件属性，业务部门自行填充
}

func zpEventCheck(zpId, appId, eventName string, timestamp int64) error {
	if zpId == "" {
		return fmt.Errorf("zpId不能为空")
	}
	if appId == "" {
		return fmt.Errorf("appId不能为空")
	}
	if eventName == "" {
		return fmt.Errorf("eventName不能为空")
	}
	if timestamp < 1606967638000 {
		return fmt.Errorf("timestamp应该是当前毫秒级时间戳")
	}
	return nil
}

func (self *ZPEventKafkaProducer) BuildZPEvent(zpId, appId, eventName, ip, addr, appUser, zpUser string,
	timestamp int64, properties map[string]string) (*ZPEvent, error) {

	if err := zpEventCheck(zpId, appId, eventName, timestamp); err != nil {
		return nil, err
	}
	if len(properties) == 0 {
		return nil, fmt.Errorf("properties is not allowed empty")
	}

	e := &ZPEvent{
		ZpId:       zpId,
		Timestamp:  timestamp,
		EventName:  eventName,
		AppID:      appId,
		Ip:         ip,
		Addr:       addr,
		AppUser:    appUser,
		ZpUser:     zpUser,
		Properties: properties,
	}
	return e, nil
}
