package main

import (
    "fmt"
    "strconv"
    "agent_pkg"
)

func GetConf () {
    agent_pkg.InitEtcdCli()
    getConf := agent_pkg.EtcdGet("apt/agent/conf")
    agent_pkg.ParseConf(getConf)
    agent_pkg.GetLocalhost()
    agent_pkg.GetPartition()
}

func GetStatus () {
    fmt.Println("Localhost:", agent_pkg.Localhost)
    status := agent_pkg.EtcdGet("apt/agent/status/" + agent_pkg.Localhost)

    if len(status) == 0 {
        agent_pkg.InitWafVds()
    } else {
        agent_pkg.UpdateWafVds(status) 
    }
}

func Kafka () {
    agent_pkg.InitBroker(agent_pkg.Localhost)
    agent_pkg.UpdateOffset()
    agent_pkg.InitConsumers(agent_pkg.Partition)
}

func Cache () {
    agent_pkg.InitCacheInfoMap()
    agent_pkg.InitCacheDataMap()
    agent_pkg.InitPrefetchMsgSwitchMap()
}

func Hdfs () {
    agent_pkg.InitHdfsCli("192.168.1.108:8020")
    agent_pkg.HdfsToLocals()
}

func Log () {
    agent_pkg.InitLog()
}

func Listen () {
    agent_pkg.ListenReq(":" + strconv.Itoa(agent_pkg.AgentConf.EngineReqPort))
}

func main () {
    Log()
    GetConf()
    GetStatus()
    Kafka()
    Cache()
    Hdfs()
    go agent_pkg.Manage()
    go agent_pkg.InitPrefetch()
    go agent_pkg.Record(3)
    Listen()
}
