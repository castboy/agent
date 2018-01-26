package main

import (
	. "agent_pkg"
	"time"
        "fmt"
        "github.com/widuu/goini"
        "strconv"
        "runtime"
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
	go TimingGetOfflineMsg(AgentConf.GetOfflineMsg)
	go ReqCount()

	time.Sleep(time.Duration(5) * time.Second)
	Listen()
}
