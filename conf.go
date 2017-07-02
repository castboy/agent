package main

import (
    "encoding/json"
    "agent_pkg"
)

func SetConf(partitions map[string]int32) string {
    partitions["192.168.1.103"] = 0
    partitions["192.168.1.104"] = 1
    partitions["192.168.1.105"] = 2
    partitions["192.168.1.106"] = 3

    topic := []string{"xdrHttp", "xdrFile"}

    conf := agent_pkg.Conf{8091, 10, partitions, topic} 

    byte, _ := json.Marshal(conf)
    
    return string(byte)
}

func main() {
    partitions := make(map[string]int32)
    setConf := SetConf(partitions)
    agent_pkg.InitEtcdCli()
    agent_pkg.EtcdSet("apt/agent/conf", setConf)

    for key, _ := range partitions {
        agent_pkg.EtcdSet("apt/agent/status/"+key, "")         
    }
}

