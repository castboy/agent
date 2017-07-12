package main

import (
	"agent_pkg"
	"encoding/json"
	"strconv"

	"github.com/widuu/goini"
)

func SetConf(port int, cache int, partitions map[string]int32, wafTopic string, vdsTopic string) string {
	topic := []string{wafTopic, vdsTopic}

	conf := agent_pkg.Conf{port, cache, partitions, topic}

	byte, _ := json.Marshal(conf)

	return string(byte)
}

func main() {
	conf := goini.SetConfig("conf.ini")
	confList := conf.ReadList()

	port, _ := strconv.Atoi(confList[7]["other"]["port"])
	cache, _ := strconv.Atoi(confList[7]["other"]["cache"])
	wafTopic := confList[0]["onlineTopic"]["waf"]
	vdsTopic := confList[0]["onlineTopic"]["vds"]
	endPoint := confList[6]["etcd"]["endPoint"]

	var partitions = make(map[string]int32)
	for key, val := range confList[5]["partition"] {
		partition, _ := strconv.Atoi(val)
		partitions[key] = int32(partition)
	}

	setConf := SetConf(port, cache, partitions, wafTopic, vdsTopic)
	agent_pkg.InitEtcdCli(endPoint)
	agent_pkg.EtcdSet("apt/agent/conf", setConf)

	for key, _ := range partitions {
		agent_pkg.EtcdSet("apt/agent/status/"+key, "")
	}
}
