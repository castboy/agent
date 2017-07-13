package main

import (
	"agent_pkg"
	"fmt"
	"strconv"

	"github.com/widuu/goini"
)

func GetConf() {
	conf := goini.SetConfig("conf.ini")
	endPoint := conf.GetValue("etcd", "endPoint")

	agent_pkg.InitEtcdCli(endPoint)
	getConf := agent_pkg.EtcdGet("apt/agent/conf")
	agent_pkg.ParseConf(getConf)
	agent_pkg.GetLocalhost()
	agent_pkg.GetPartition()
}

func GetStatus() {
	fmt.Println("Localhost:", agent_pkg.Localhost)
	status := agent_pkg.EtcdGet("apt/agent/status/" + agent_pkg.Localhost)

	if len(status) == 0 {
		agent_pkg.InitWafVds()
	} else {
		agent_pkg.UpdateWafVds(status)
	}
}

func Kafka() {
	agent_pkg.InitBroker(agent_pkg.Localhost)
	agent_pkg.UpdateOffset()
	agent_pkg.InitConsumers(agent_pkg.Partition)
}

func Cache() {
	agent_pkg.InitCacheInfoMap()
	agent_pkg.InitCacheDataMap()
	agent_pkg.InitPrefetchMsgSwitchMap()
}

func Hdfs() {
	conf := goini.SetConfig("conf.ini")
	nameNode := conf.GetValue("hdfs", "nameNode")

	agent_pkg.InitHdfsCli(nameNode)
	agent_pkg.HdfsToLocals()
}

func Log() {
	agent_pkg.InitLog()
}

func Listen() {
	agent_pkg.ListenReq(":" + strconv.Itoa(agent_pkg.AgentConf.EngineReqPort))
}

func main() {
	Log()
	GetConf()
	GetStatus()
	Kafka()
	Cache()
	Hdfs()
	go agent_pkg.Manage()
	go agent_pkg.InitPrefetch()
	go agent_pkg.Record(3)
	go agent_pkg.SendClearFileHdlMsg(20)
	Listen()
}
