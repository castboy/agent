package main

import (
	. "agent_pkg"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/widuu/goini"
)

func cpuNum() int {
	conf := goini.SetConfig("conf.ini")
	cpu, err := strconv.Atoi(conf.GetValue("other", "cpu"))
	if nil != err {
		fmt.Println(err.Error())
	}

	return cpu
}

func main() {
	num := cpuNum()
	if -1 != num {
		runtime.GOMAXPROCS(num)
	}

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
