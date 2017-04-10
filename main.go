package main

import (
    "pkg_wmg"
)

func main() {
    pkg_wmg.Read("json.txt")
    pkg_wmg.InitWafVds()
    pkg_wmg.InitBroker()
    pkg_wmg.UpdateOffset()
    pkg_wmg.InitConsumers()
    pkg_wmg.InitWafVdsBak()
    pkg_wmg.ListenHttp() 
 
}
