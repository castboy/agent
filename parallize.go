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
    go pkg_wmg.Manage()
    go pkg_wmg.InitPrefetch()
    pkg_wmg.Listen()
}
