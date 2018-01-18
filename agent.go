package main

import (
	. "agent_pkg"
	"time"
)

func main() {
	InitLog("run/log")
	GetConf()
	InitBroker()
	RightStatus()
	Kafka()
	Buffer()
	Hdfs()
	go Manage()
	go InitPrefetch()
	go SendClearFileHdlMsg(AgentConf.ClearHdfsHdl)
	go ReHdfsCli()
	CompensationOfflineMsg()
	go TimingGetOfflineMsg(AgentConf.GetOfflineMsg)
	go ReqCount()

	time.Sleep(time.Duration(3) * time.Second)
	Listen()
}
