package main

import (
	"agent_pkg"
	"encoding/json"
	"log"
	"strconv"

	"github.com/widuu/goini"
)

func SetConf(port, cache int, partitions map[string]int32, wafTopic, vdsTopic, nameNode, webServerIp string,
	webServerPort int, wafInstanceSrc, wafInstanceDst string, offlineMsgTopic string, offlineMsgPartition int) string {
	topic := []string{wafTopic, vdsTopic}

	conf := agent_pkg.Conf{port, cache, partitions, topic, nameNode, webServerIp,
		webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition}

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
	endPoint := conf.GetValue("etcd", "endPoint")
	nameNode := conf.GetValue("hdfs", "nameNode")

	wafInstanceSrc := conf.GetValue("wafInstance", "src")
	wafInstanceDst := conf.GetValue("wafInstance", "dst")

	offlineMsgTopic := conf.GetValue("offlineMsg", "topic")
	offlineMsgPartition, err := strconv.Atoi(conf.GetValue("offlineMsg", "partition"))
	if nil != err {
		log.Fatal("offlineMsgPartition config err")
	}

	webServerIp := conf.GetValue("webServer", "ip")
	webServerPort, err := strconv.Atoi(conf.GetValue("webServer", "port"))
	if nil != err {
		log.Fatal("webServerPort config err")
	}

	var partitions = make(map[string]int32)
	for key, val := range confList[0]["partition"] {
		partition, err := strconv.Atoi(val)
		if nil != err {
			log.Fatal("partition config err")
		}
		partitions[key] = int32(partition)
	}

	setConf := SetConf(port, cache, partitions, wafTopic, vdsTopic, nameNode,
		webServerIp, webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition)
	agent_pkg.InitEtcdCli(endPoint)
	agent_pkg.EtcdSet("apt/agent/conf", setConf)
}
