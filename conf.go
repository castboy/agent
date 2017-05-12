package main

import (
    "encoding/json"
    "agent_pkg"
)

func SetConf() string {
    partitions := make(map[string]int32)
    partitions["10.80.6.7"] = 0
    partitions["10.80.6.8"] = 1
    partitions["10.80.6.9"] = 2
    partitions["10.80.6.10"] = 3

    topic := []string{"xdrHttp", "xdrFile"}

    conf := agent_pkg.Conf{8081, 100, partitions, topic} 

    byte, _ := json.Marshal(conf)
    
    return string(byte)
}

func main() {
    setConf := SetConf()
    agent_pkg.InitEtcdCli()
    agent_pkg.EtcdSet("apt/agent/conf", setConf)
}

