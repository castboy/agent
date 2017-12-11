package main

import (
	. "agent_pkg"
	"encoding/json"
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

	conf := Conf{port, cache, partitions, topic, offset, nameNode, webServerIp,
		webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset}

	byte, err := json.Marshal(conf)
	if nil != err {
		Log("CRT", "json.Marshal err on %v in SetConf", conf)
	}

	return string(byte)
}

func main() {
	conf := goini.SetConfig("conf.ini")
	confList := conf.ReadList()

	port, err := strconv.Atoi(conf.GetValue("other", "port"))
	if nil != err {
		Log("CRT", "%s", "port config err")
	}

	cache, err := strconv.Atoi(conf.GetValue("other", "cache"))
	if nil != err {
		Log("CRT", "%s", "cache config err")
	}

	wafTopic := conf.GetValue("onlineTopic", "waf")
	vdsTopic := conf.GetValue("onlineTopic", "vds")

	wafOffset, err := strconv.ParseInt(conf.GetValue("onlineOffset", "waf"), 10, 64)
	if nil != err {
		Log("CRT", "%s", "wafOffset config err")
	}
	vdsOffset, err := strconv.ParseInt(conf.GetValue("onlineOffset", "vds"), 10, 64)
	if nil != err {
		Log("CRT", "%s", "vdsOffset config err")
	}

	nameNode := conf.GetValue("hdfs", "nameNode")

	wafInstanceSrc := conf.GetValue("wafInstance", "src")
	wafInstanceDst := conf.GetValue("wafInstance", "dst")

	offlineMsgTopic := conf.GetValue("offlineMsg", "topic")
	offlineMsgPartition, err := strconv.Atoi(conf.GetValue("offlineMsg", "partition"))
	if nil != err {
		Log("CRT", "%s", "offlineMsgPartition config err")
	}
	offlineMsgStartOffset, err := strconv.Atoi(conf.GetValue("offlineMsg", "startOffset"))
	if nil != err {
		Log("CRT", "%s", "offlineMsgstartOffset config err")
	}

	webServerIp := conf.GetValue("webServer", "ip")
	webServerPort, err := strconv.Atoi(conf.GetValue("webServer", "port"))
	if nil != err {
		Log("CRT", "%s", "webServerPort config err")
	}

	var partitions = make(map[string]int32)
	for key, val := range confList[0]["partition"] {
		partition, err := strconv.Atoi(val)
		if nil != err {
			Log("CRT", "%s", "partition config err")
		}
		partitions[key] = int32(partition)
	}

	EtcdNodes = confList[1]["etcd"]

	setConf := SetConf(port, cache, partitions, wafTopic, vdsTopic, wafOffset, vdsOffset, nameNode, webServerIp,
		webServerPort, wafInstanceSrc, wafInstanceDst, offlineMsgTopic, offlineMsgPartition, offlineMsgStartOffset)

	InitEtcdCli()
	EtcdSet("apt/agent/conf", setConf)
}
