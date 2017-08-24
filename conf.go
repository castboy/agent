package main

import (
	"agent_pkg"
	"encoding/json"
	"log"
	"strconv"

	"github.com/widuu/goini"
)

func SetConf(port, cache int, partitions map[string]int32,
	wafTopic, vdsTopic string, wafOffset, vdsOffset int64,
	nameNode, webServerIp string, webServerPort int,
	wafInstanceSrc, wafInstanceDst string, offlineMsgTopic string,
	offlineMsgPartition, offlineMsgStartOffset int) string {
	topic := []string{wafTopic, vdsTopic}
	offset := []int64{wafOffset, vdsOffset}

	conf := agent_pkg.Conf{port, cache, partitions, topic, offset, nameNode, webServerIp,
		webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset}

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

	var partitions = make(map[string]int32)
	for key, val := range confList[0]["partition"] {
		partition, err := strconv.Atoi(val)
		if nil != err {
			log.Fatal("partition config err")
		}
		partitions[key] = int32(partition)
	}

	endPoints := confList[0]["etcd"]

	setConf := SetConf(port, cache, partitions, wafTopic, vdsTopic, wafOffset, vdsOffset, nameNode, webServerIp,
		webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset)
	agent_pkg.InitEtcdCli(endPoints)
	agent_pkg.EtcdSet("apt/agent/conf", setConf)
}
