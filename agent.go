package main

import (
	. "agent_pkg"
	"time"
)

func main() {
	LimitCpuNum()
	InitLog("run/log")
	InitLogXdrErr("run/xdr/log")
	GetConf()
	InitBroker()
	RightStatus()
	Kafka()
	Buffer()
	Hdfs()
	go Manage()
	go InitPrefetch()
	go SendClearFileHdlMsg(AgentConf.ClearHdfsHdl)
	CompensationOfflineMsg()

	go CollectOfflineMsgExedRes()
	go MsgExeVerify()
	go ManageHeartBeat()
	go TimingGetOfflineMsg(AgentConf.GetOfflineMsg)
	go ReqCount()

	time.Sleep(time.Duration(5) * time.Second)
	ResetOffsetInConfFile()
	Listen()
}
