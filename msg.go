package main

import (
	//	"fmt"
	"encoding/json"
	"log"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type OfflineMsg struct {
	Engine     string `json: "Engine"`
	Topic      string `json: "Topic"`
	Weight     int    `json: "Weight"`
	SignalType string `json: "SignalType"`
}

const (
	topic     = "offline_msg"
	partition = 0
)

var kafkaAddrs = []string{"192.168.1.103:9092", "192.168.1.103:9093"}

func main() {
	var msgs [5]OfflineMsg
	msgs[0] = OfflineMsg{"vds", "vds-alert", 10, "start"}
	msgs[1] = OfflineMsg{"vds", "vds-alert", 10, "shutdown"}
	msgs[2] = OfflineMsg{"vds", "vds-alert", 10, "start"}
	msgs[3] = OfflineMsg{"vds", "vds-alert", 10, "stop"}
	msgs[4] = OfflineMsg{"waf", "waf-alert", 10, "start"}

	conf := kafka.NewBrokerConf("test-client")
	conf.AllowTopicCreation = true

	broker, err := kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		log.Fatalf("cannot connect to kafka cluster: %s", err)
	}
	defer broker.Close()

	producer := broker.Producer(kafka.NewProducerConf())

	for _, v := range msgs {
		bytes, err := json.Marshal(v)
		if nil != err {
			log.Fatalln("json.Marshal err")
		}

		msg := &proto.Message{Value: bytes}

		if _, err := producer.Produce(topic, partition, msg); err != nil {
			log.Fatalf("cannot produce message to %s:%d: %s", topic, partition, err)
		}
	}

}
