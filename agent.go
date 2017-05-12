package main

import (
    "fmt"
    "strconv"
    "agent_pkg"
)

func GetConf() {
    agent_pkg.InitEtcdCli()
    getConf := agent_pkg.EtcdGet("apt/agent/conf")
    agent_pkg.ParseConf(getConf)
    agent_pkg.GetLocalhost()
    agent_pkg.GetPartition()
}

func GetStatus() {
    status := agent_pkg.EtcdGet("apt/agent/status/" + agent_pkg.Localhost)

    fmt.Println(string(status))
    if len(status) == 0 {
        agent_pkg.InitWafVds()
    } else {
        agent_pkg.UpdateWafVds(status) 
    }
    //fmt.Println("Waf:", Waf)
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

func Listen() {
    agent_pkg.ListenReq(":" + strconv.Itoa(agent_pkg.AgentConf.EngineReqPort))
}

func main() {
    GetConf()
    GetStatus()
    Kafka()
    Cache()
    go agent_pkg.Manage()
    go agent_pkg.InitPrefetch()
    go agent_pkg.Record()
    Listen()
}
