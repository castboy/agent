package main

import (
	"agent_pkg"
	"log"
	"strconv"

	"github.com/widuu/goini"
)

func GetConf() {
	conf := goini.SetConfig("conf.ini")
	endPoint := conf.GetValue("etcd", "endPoint")

	agent_pkg.InitEtcdCli(endPoint)
	getConf, ok := agent_pkg.EtcdGet("apt/agent/conf")
	if !ok {
		log.Fatal("configurations does not exist")
	}

	agent_pkg.ParseConf(getConf)
	agent_pkg.GetLocalhost()
	agent_pkg.GetPartition()
}

func SetStatus() {
	status, ok := agent_pkg.EtcdGet("apt/agent/status/" + agent_pkg.Localhost)

	if !ok {
		agent_pkg.InitStatus()
	} else {
		agent_pkg.GetStatusFromEtcd(status)
	}
}

func Kafka() {
	agent_pkg.InitBroker(agent_pkg.Localhost)
	agent_pkg.InitConsumers(agent_pkg.Partition)
	agent_pkg.UpdateOffset()
}

func Cache() {
	agent_pkg.InitBuffersStatus()
	agent_pkg.InitBuffer()
	agent_pkg.InitPrefetchMsgSwitchMap()
}

func Hdfs() {
	agent_pkg.InitHdfsCli(agent_pkg.AgentConf.HdfsNameNode)
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
	SetStatus()
	Kafka()
	Cache()
	Hdfs()
	go agent_pkg.Manage()
	go agent_pkg.InitPrefetch()
	go agent_pkg.SendClearFileHdlMsg(20)
	agent_pkg.SetStatus()
	go agent_pkg.TimingGetOfflineMsg(3)
	Listen()
}
