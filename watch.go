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
	Engine     string `json: "Engine"`
	Topic      string `json: "Topic"`
	Weight     int    `json: "Weight"`
	SignalType string `json: "SignalType"`
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
	conf := goini.SetConfig("conf.ini")
	port := conf.GetValue("other", "port")

	url := fmt.Sprintf("http://%s:%s/offline", ip, port)

	switch msg.SignalType {
	case "start":
		params := fmt.Sprintf("signal=%s&type=%s&topic=%s&weight=%d",
			msg.SignalType, msg.Engine, msg.Topic, strconv.Itoa(msg.Weight))
		_, err = http.Get(url + "?" + params)
		break

	case "stop":
		params := fmt.Sprintf("signal=%s&type=%s&topic=%s",
			msg.SignalType, msg.Engine, msg.Topic)
		_, err = http.Get(url + "?" + params)
		break

	case "shutdown":
		params := fmt.Sprintf("signal=%s&type=%s&topic=%s",
			msg.SignalType, msg.Engine, msg.Topic)
		_, err = http.Get(url + "?" + params)
		break

	case "complete":
		params := fmt.Sprintf("signal=%s&type=%s&topic=%s",
			msg.SignalType, msg.Engine, msg.Topic)
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
	for key, val := range confList[0]["partition"] {
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
