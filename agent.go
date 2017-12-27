package main

import (
	. "agent_pkg"
	"time"
)

func main() {
	InitLog()
	GetConf()
	InitBroker()
	RightStatus()
	Kafka()
	Buffer()
	Hdfs()
	go Manage()
	go InitPrefetch()
	go SendClearFileHdlMsg(20)
	CompensationOfflineMsg()
	go TimingGetOfflineMsg(3)
	go ReqCount()

	time.Sleep(time.Duration(1) * time.Second)
	Listen()
}
