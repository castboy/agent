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
            //fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
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

    if msg.SignalType == "start" {
        url := "http://" + ip + ":8081/start"
        params := "type=" + msg.Engine + "&topic=" + msg.Topic + "&weight=" +strconv.Itoa(msg.Weight)
        //fmt.Println(params)
        _, err = http.Get(url + "?" + params)
    } else {
        url := "http://" + ip + ":8081/stop"
        params := "type=" + msg.Engine + "&topic=" + msg.Topic
        _, err = http.Get(url + "?" + params)
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
