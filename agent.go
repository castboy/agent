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
	CompensationOfflineMsg()
	go TimingGetOfflineMsg(AgentConf.GetOfflineMsg)
	go ReqCount()

	time.Sleep(time.Duration(AgentConf.DelayListen) * time.Second)
	Listen()
}
