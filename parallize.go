package main

import (
    //"fmt"
    "pkg_wmg"
)

func main() {
    pkg_wmg.Read("json.v2")
    pkg_wmg.InitWafVds()
    pkg_wmg.InitBroker()
    pkg_wmg.UpdateOffset()
    pkg_wmg.InitConsumers()
    pkg_wmg.InitCacheInfoMap()
    pkg_wmg.InitPrefetchMsgSwitchMap()
    go pkg_wmg.Manage()
    go pkg_wmg.InitPrefetch()
    go pkg_wmg.Record("record")
    pkg_wmg.Listen()
}
