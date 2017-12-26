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
	SetStatus()
	go TimingGetOfflineMsg(3)

	time.Sleep(time.Duration(1) * time.Second)
	Listen()
}
