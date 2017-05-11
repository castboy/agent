package main

import (
    //"fmt"
    "agent_pkg"
)

func Etcd() {
    setConf := agent_pkg.SetConf()
    agent_pkg.InitEtcdCli()
    agent_pkg.EtcdSet("apt/agent/conf", setConf)
    getConf := agent_pkg.EtcdGet("apt/agent/conf")
    agent_pkg.ParseConf(getConf)
}

func main() {
    Etcd()
    agent_pkg.Read("data.init")
    agent_pkg.InitWafVds()
    agent_pkg.InitConf("conf.data")
    agent_pkg.InitBroker()
    agent_pkg.UpdateOffset()
    agent_pkg.InitConsumers()
    agent_pkg.InitCacheInfoMap()
    agent_pkg.InitCacheDataMap()
    agent_pkg.InitPrefetchMsgSwitchMap()
    go agent_pkg.Manage()
    go agent_pkg.InitPrefetch()
    go agent_pkg.Record("data.record")
    agent_pkg.Listen(":8081")
}
