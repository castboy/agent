Skip to content
This repository
Search
Pull requests
Issues
Marketplace
Gist
 @castboy
 Sign out
 Unwatch 1
  Star 0
 Fork 0 castboy/agent
 Code  Issues 0  Pull requests 0  Projects 0  Wiki  Settings Insights 
Branch: v2.2 Find file Copy pathagent/watch.go
e5c9804  29 minutes ago
 castboy right watch.go
0 contributors
RawBlameHistory     
94 lines (77 sloc)  2.18 KB
package main

import (
    "log"
    "time"
    "encoding/json"
    "net/http"
    "fmt"
    "strconv"
    "golang.org/x/net/context"
    "github.com/coreos/etcd/clientv3"
)

type OfflineMsg struct {
    Engine string
    Topic string
    Weight int
    SignalType string
}

var offlineMsg OfflineMsg
var AgentHosts  = [2]string{"10.88.1.103", "10.88.1.104"}

func Watch() {
    cfg := clientv3.Config{
        Endpoints:               []string{"http://10.88.1.103:2379"},
        DialTimeout: 5 * time.Second,
    }

    cli, err := clientv3.New(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    rch := cli.Watch(context.Background(), "apt/agent/offlineReq/", clientv3.WithPrefix())
    for wresp := range rch {
        for _, ev := range wresp.Events {
            fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
            fmt.Println(string(ev.Kv.Value))
            ParseOfflineMsg(ev.Kv.Value)
            PushOfflineMsg()
        }
    }
}

func ParseOfflineMsg (msg []byte) {
    err := json.Unmarshal(msg, &offlineMsg)
    if err != nil {
        fmt.Println(offlineMsg)
    } 
}

func HttpGet (ip string, msg OfflineMsg) {
    var err error
    url := "http://" + ip + ":8081/offline"

    switch msg.SignalType {
        case "start":
            params := "type=" + msg.SignalType + "&engine=" + msg.Engine + "&topic=" + msg.Topic + "&weight=" +strconv.Itoa(msg.Weight)
            _, err = http.Get(url + "?" + params)
            break

        case "stop":
            params := "type=" + msg.SignalType + "&engine=" + msg.Engine + "&topic=" + msg.Topic
            _, err = http.Get(url + "?" + params)
            break

        case "shutdown":
            params := "type=" + msg.SignalType + "&engine=" + msg.Engine + "&topic=" + msg.Topic
            _, err = http.Get(url + "?" + params)
            break

        case "complete":
            params := "type=" + msg.SignalType + "&engine=" + msg.Engine + "&topic=" + msg.Topic
            _, err = http.Get(url + "?" + params)
            break
    }

    if err != nil {
    }
}

func PushOfflineMsg () {
    for _, ip := range AgentHosts {
        HttpGet(ip, offlineMsg)
    }
}

func main () {
    Watch()
}
