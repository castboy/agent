package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/optiopay/kafka"
)

type OfflineMsg struct {
	Engine     string `json: "Engine"`
	Topic      string `json: "Topic"`
	Weight     int    `json: "Weight"`
	SignalType string `json: "SignalType"`
}

var broker kafka.Client

func InitBroker() {
	var kafkaAddrs []string = []string{"192.168.1.103" + ":9092", "192.168.1.103" + ":9093"}
	conf := kafka.NewBrokerConf("agent")
	conf.AllowTopicCreation = false

	var err error
	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		log.Fatalf("cannot connect to kafka cluster: %s", err)
	}

	defer broker.Close()
}

func InitConsumer(topic string, partition int32, start int64) (kafka.Consumer, error) {
	conf := kafka.NewConsumerConf(topic, partition)
	conf.StartOffset = start
	conf.RetryLimit = 1
	consumer, err := broker.Consumer(conf)

	if err != nil {
	}

	return consumer, err
}

func SetStatus() {
	msgs := LoadOfflineMsg()
	msgs = ExtractValidOfflineMsg(msgs)
}

func LoadOfflineMsg() (offlineMsgs []OfflineMsg) {
	var msg OfflineMsg

	consumer, err := InitConsumer("offline_msg", 0, 0)
	if nil != err {
		info := "init TimingGetOfflineMsg consumer err"
		log.Fatalln(info)
	}

	for {
		kafkaMsg, err := consumer.Consume()
		if nil != err {
			break
		} else {
			err := json.Unmarshal(kafkaMsg.Value, &msg)
			if nil != err {
			} else {
				offlineMsgs = append(offlineMsgs, msg)
			}
		}
	}

	fmt.Println("all msg:", offlineMsgs)
	return offlineMsgs
}

func ExtractValidOfflineMsg(offlineMsgs []OfflineMsg) []OfflineMsg {
	var invalidOfflineTask []string
	var invalidOfflineTaskId []int
	var validOfflineMsg []OfflineMsg

	for _, v := range offlineMsgs {
		if "shutdown" == v.SignalType {
			invalidOfflineTask = append(invalidOfflineTask, v.Topic)
		}
	}

	for i, j := range offlineMsgs {
		for _, v := range invalidOfflineTask {
			if j.Topic == v {
				invalidOfflineTaskId = append(invalidOfflineTaskId, i)
			}
		}
	}

	for k, _ := range offlineMsgs {
		valid := true
		for _, j := range invalidOfflineTaskId {
			if j == k {
				valid = false
			}
		}
		if valid {
			validOfflineMsg = append(validOfflineMsg, offlineMsgs[k])
		}
	}

	fmt.Println("valid msg:", validOfflineMsg)
	return validOfflineMsg

}

func main() {
	InitBroker()
	SetStatus()
}
