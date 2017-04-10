package main

import (
	"log"
	"github.com/optiopay/kafka"
	"io"
    "pkg_wmg"
)

const (
	topic     = "kafka-conn-go"
	partition = 0
)

var kafkaAddrs = []string{"10.80.6.9:9092", "10.80.6.9:9093"}
var consumer kafka.Consumer
var broker kafka.Client

func ReadData (w http.ResponseWriter, r *http.Request) {
		//io.WriteString(w, "waiting data ...\n")
		log.Println("waiting produce data ...")
		msg, err := consumer.Consume()
		if err != nil {
			if err != kafka.ErrNoData {
				log.Printf("cannot consume %q topic message: %s", topic, err)
			}
		}
		log.Printf("message %d: %s", msg.Offset, msg.Value)
		io.WriteString(w, "received:"+string(msg.Value)+"\n")
}

// printConsumed read messages from kafka and print them out
func printConsumed() {
	conf := kafka.NewConsumerConf(topic, partition)
	conf.StartOffset = kafka.StartOffsetNewest
	consumer, _ = broker.Consumer(conf)
//	if err != nil {
//		log.Fatalf("cannot create kafka consumer for %s:%d: %s", topic, partition, err)
//	}
}

func InitBroker () {
	conf := kafka.NewBrokerConf("wmg-test-client")
	conf.AllowTopicCreation = true

	// connect to kafka cluster
    var err error
	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		log.Fatalf("cannot connect to kafka cluster: %s", err)
	}
	defer broker.Close()
}

func main() {
    InitBroker()
	printConsumed()
      
    pkg_wmg.ListenDataReq() 
 
}
