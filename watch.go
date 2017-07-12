package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/widuu/goini"
	"golang.org/x/net/context"
)

type OfflineMsg struct {
	Engine     string
	Topic      string
	Weight     int
	SignalType string
}

var offlineMsg OfflineMsg

func Watch() {
	conf := goini.SetConfig("conf.ini")
	endPoint := conf.GetValue("etcd", "endPoint")

	cfg := clientv3.Config{
		Endpoints:   []string{"http://" + endPoint + ":2379"},
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

func ParseOfflineMsg(msg []byte) {
	err := json.Unmarshal(msg, &offlineMsg)
	if err != nil {
		fmt.Println(offlineMsg)
	}
}

func HttpGet(ip string, msg OfflineMsg) {
	var err error
	url := "http://" + ip + ":8081/offline"

	switch msg.SignalType {
	case "start":
		params := "type=" + msg.SignalType + "&engine=" + msg.Engine + "&topic=" + msg.Topic + "&weight=" + strconv.Itoa(msg.Weight)
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

func PushOfflineMsg() {
	conf := goini.SetConfig("conf.ini")
	confList := conf.ReadList()

	var partitions = make(map[string]int32)
	for key, val := range confList[5]["partition"] {
		partition, _ := strconv.Atoi(val)
		partitions[key] = int32(partition)
	}
	for ip, _ := range partitions {
		HttpGet(ip, offlineMsg)
	}
}

func main() {
	Watch()
}
