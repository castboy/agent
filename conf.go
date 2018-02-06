package main

import (
	. "agent_pkg"
	"encoding/json"
	"strconv"

	"github.com/widuu/goini"
)

func SetConf(conf Conf) string {
	byte, err := json.Marshal(conf)
	if nil != err {
		LogCrt("json.Marshal err on %v in SetConf", conf)
	}

	return string(byte)
}

func main() {
	InitLog("conf/log")

	conf := goini.SetConfig("conf.ini")
	confList := conf.ReadList()

	port, err := strconv.Atoi(conf.GetValue("other", "port"))
	if nil != err {
		LogCrt("%s", "port config err")
	}

	cache, err := strconv.Atoi(conf.GetValue("other", "cache"))
	if nil != err {
		LogCrt("%s", "cache config err")
	}

	wafTopic := conf.GetValue("onlineTopic", "waf")
	vdsTopic := conf.GetValue("onlineTopic", "vds")

	nameNode := conf.GetValue("hdfs", "nameNode")

	wafInstanceSrc := conf.GetValue("wafInstance", "src")
	wafInstanceDst := conf.GetValue("wafInstance", "dst")

	offlineMsgTopic := conf.GetValue("offlineMsg", "topic")
	offlineMsgPartition, err := strconv.Atoi(conf.GetValue("offlineMsg", "partition"))
	if nil != err {
		LogCrt("%s", "offlineMsgPartition config err")
	}

	webServerIp := conf.GetValue("webServer", "ip")
	webServerPort, err := strconv.Atoi(conf.GetValue("webServer", "port"))
	if nil != err {
		LogCrt("%s", "webServerPort config err")
	}

	var partitions = make(map[string]int32)
	for key, val := range confList[0]["partition"] {
		partition, err := strconv.Atoi(val)
		if nil != err {
			LogCrt("%s", "partition config err")
		}
		partitions[key] = int32(partition)
	}

	EtcdNodes = confList[1]["etcd"]

	clearHdfsHdl, err := strconv.Atoi(conf.GetValue("timer", "clearHdfsHdl"))
	getOfflineMsg, err := strconv.Atoi(conf.GetValue("timer", "getOfflineMsg"))

	cnf := Conf{
		EngineReqPort:     port,
		MaxCache:          cache,
		Partition:         partitions,
		Topic:             []string{wafTopic, vdsTopic},
		HdfsNameNode:      nameNode,
		WebServerReqIp:    webServerIp,
		WebServerReqPort:  webServerPort,
		WafInstanceSrc:    wafInstanceSrc,
		WafInstanceDst:    wafInstanceDst,
		OfflineMsgTopic:   offlineMsgTopic,
		OfflineMsgPartion: offlineMsgPartition,
		ClearHdfsHdl:      clearHdfsHdl,
		GetOfflineMsg:     getOfflineMsg,
	}

	setConf := SetConf(cnf)

	InitEtcdCli()
	EtcdSet("apt/agent/conf", setConf)
}
