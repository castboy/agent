package main

import (
	"agent_pkg"
	"encoding/json"
	"log"
	"strconv"

	"github.com/widuu/goini"
)

func SetConf(port, cache int,
	wafTopic, vdsTopic string, wafOffset, vdsOffset int64,
	nameNode, webServerIp string, webServerPort int,
	wafInstanceSrc, wafInstanceDst string, offlineMsgTopic string,
	offlineMsgPartition, offlineMsgStartOffset int, kafkaHost string, kafkaPartition int32) string {
	topic := []string{wafTopic, vdsTopic}
	offset := []int64{wafOffset, vdsOffset}

	conf := agent_pkg.Conf{port, cache, topic, offset, nameNode, webServerIp, webServerPort, wafInstanceSrc,
		wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset, kafkaHost, kafkaPartition}

	byte, err := json.Marshal(conf)
	if nil != err {
		log.Fatal("json err")
	}

	return string(byte)
}

func main() {
	conf := goini.SetConfig("conf.ini")
	confList := conf.ReadList()

	port, err := strconv.Atoi(conf.GetValue("other", "port"))
	if nil != err {
		log.Fatal("port config err")
	}

	cache, err := strconv.Atoi(conf.GetValue("other", "cache"))
	if nil != err {
		log.Fatal("cache config err")
	}

	wafTopic := conf.GetValue("onlineTopic", "waf")
	vdsTopic := conf.GetValue("onlineTopic", "vds")

	wafOffset, err := strconv.ParseInt(conf.GetValue("onlineOffset", "waf"), 10, 64)
	if nil != err {
		log.Fatal("wafOffset config err")
	}
	vdsOffset, err := strconv.ParseInt(conf.GetValue("onlineOffset", "vds"), 10, 64)
	if nil != err {
		log.Fatal("vdsOffset config err")
	}

	nameNode := conf.GetValue("hdfs", "nameNode")

	wafInstanceSrc := conf.GetValue("wafInstance", "src")
	wafInstanceDst := conf.GetValue("wafInstance", "dst")

	offlineMsgTopic := conf.GetValue("offlineMsg", "topic")
	offlineMsgPartition, err := strconv.Atoi(conf.GetValue("offlineMsg", "partition"))
	if nil != err {
		log.Fatal("offlineMsgPartition config err")
	}
	offlineMsgStartOffset, err := strconv.Atoi(conf.GetValue("offlineMsg", "startOffset"))
	if nil != err {
		log.Fatal("offlineMsgstartOffset config err")
	}

	webServerIp := conf.GetValue("webServer", "ip")
	webServerPort, err := strconv.Atoi(conf.GetValue("webServer", "port"))
	if nil != err {
		log.Fatal("webServerPort config err")
	}

	endPoints := confList[1]["etcd"]

	kafkaHost := conf.GetValue("kafka", "host")
	partition, err := strconv.Atoi(conf.GetValue("kafka", "partition"))
	if nil != err {
		log.Fatal("kafka-partition conf err")
	}
	kafkaPartition := int32(partition)

	setConf := SetConf(port, cache, wafTopic, vdsTopic, wafOffset, vdsOffset, nameNode, webServerIp, webServerPort,
		wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset, kafkaHost, kafkaPartition)
	agent_pkg.InitEtcdCli(endPoints)
	agent_pkg.EtcdSet("apt/agent/conf", setConf)
}
