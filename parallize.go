package main

import (
    //"fmt"
    "agent_pkg"
)

func main() {
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
