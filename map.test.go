package main

import (
	"encoding/json"
    "github.com/optiopay/kafka"
    "net/http"
	"fmt"
	"io"
	"io/ioutil"
	"os"
    "strconv"
    "log"
)

var kafkaAddrs = []string{"10.80.6.9:9092", "10.80.6.90:9093"}
var broker kafka.Client
var consumer kafka.Consumer

type Partition struct {
	First   int
	Current int
	Last    int
	Weight  int
}

var wafVds [2]map[string]Partition

var waf map[string]Partition
var vds map[string]Partition
var wafWeightTotal int = 0
var vdsWeightTotal int = 0

func Read(file string) {
	waf = make(map[string]Partition, 1000)
	vds = make(map[string]Partition, 1000)
	fileHdl, err := os.OpenFile(file, os.O_RDONLY, 0666)
	if nil != err {
		fmt.Println("openfileError")
	}

	bytes, err := ioutil.ReadAll(fileHdl)
	if nil != err {
	}

	err = json.Unmarshal(bytes, &wafVds)
	if nil != err {
	}

	waf = wafVds[0]
	vds = wafVds[1]

    for _, v := range waf {
        wafWeightTotal += v.Weight  
    }
    fmt.Println(wafWeightTotal)
    for _, v := range vds {
        vdsWeightTotal += v.Weight  
    }

	defer fileHdl.Close()
}

func Write(file string) {
	fileHdl, _ := os.OpenFile(file, os.O_WRONLY, 0666)
	bytes, _ := json.Marshal(wafVds)
	io.WriteString(fileHdl, string(bytes))

	fileHdl.Close()
}

func Append(file string, pType string, pName string, pProperties Partition) {
	fileHdl, _ := os.OpenFile(file, os.O_RDWR, 0666)
	if "waf" == pType {
		waf[pName] = pProperties
	} else {
		vds[pName] = pProperties
	}
	bytes, _ := json.Marshal(wafVds)
	io.WriteString(fileHdl, string(bytes))

	fmt.Println(string(bytes))

	defer fileHdl.Close()
}

//func Delete(file string, pType string, pName string) {
//	fileHdl, _ := os.OpenFile(file, os.O_RDWR, 0666)
//	if "waf" == pType {
//		delete(waf, pName)
//	} else {
//		delete(vds, pName)
//	}
//
//	fmt.Println(wafVds)
//
//	bytes, _ := json.Marshal(wafVds)
//	io.WriteString(fileHdl, string(bytes))
//
//	fmt.Println(string(bytes))
//
//	defer fileHdl.Close()
//}

func InitBroker () {
    conf := kafka.NewBrokerConf("agent")
    conf.AllowTopicCreation = true
    var err error
    broker, err = kafka.Dial(kafkaAddrs, conf)
    if err != nil {
        log.Fatalf("cannot connect to kafka cluster: %s", err)
    }

    defer broker.Close()
}

func GetMsgStart (topic string, partition int32) int64 {
    start, err := broker.OffsetEarliest(topic, partition)
    if err != nil {
        log.Fatalf("cannot get start %s", err)
    }

    return start
}

func GetMsgEnd (topic string, partition int32) int64 {
    end, err := broker.OffsetLatest(topic, partition)
    if err != nil {
        log.Fatalf("cannot get end %s", err)
    }

    return end
}

func InitConsumer (topic string, partition int32, start int64) {
    conf := kafka.NewConsumerConf(topic, partition)
    conf.StartOffset = start
    var err error
    consumer, err = broker.Consumer(conf)
    if err != nil {
        fmt.Println("create consumer failed")    
    }
}

func ConsumeData () {
    msg, err := consumer.Consume() 
    
    if err != nil {
        log.Println("mgs err")
    } 
    log.Printf("message %d: %s", msg.Offset, msg.Value)
}

func Consume (pType string, topic string) {
    if "waf" == pType {
        waf[topic] = Partition{waf[topic].First, waf[topic].Current+1, waf[topic].Last, waf[topic].Weight}   
    }
}

func ConsumeDistri (pType string, num int) {
    if "waf" == pType {
        if num < wafWeightTotal {
            for k, _ := range waf {
                Consume(pType, k)    
            }
            for i := 0; i < num - len(waf); i++ {
                Consume(pType, "waf")    
            }
        } else {
            times, remainder := num / wafWeightTotal, num % wafWeightTotal    
            for i := 0; i < times; i++ {
                for k, v := range waf {
                    for j := 0; j < v.Weight; j++ {
                        Consume(pType, k)    
                    }    
                }    
            }
            for n := 0; n < remainder; n++ {
                Consume(pType, "waf")    
            } 
            
        }
            
    } else {

    }

}

func StartOffline (w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    pType := r.Form["type"][0]
    topic := r.Form["topic"][0]
    weight, _ := strconv.Atoi(r.Form["weight"][0])

    if "waf" == pType {
        waf[topic] = Partition{0, 0, 1000, weight}     
    }
    wafWeightTotal += weight
    ConsumeDistri("waf", 1000)

    b, _ := json.Marshal(waf)
    fmt.Fprintf(w, string(b))
}

func GetLastOffset (w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    topic := r.Form["topic"][0] 
   // endOffset := GetMsgEnd(topic, 0) 
    //fmt.Fprintf(w, string(endOffset))
    fmt.Fprintf(w, topic)
    ConsumeData()
} 

func ListenOfflineSignal () {
    OfflineMux := http.NewServeMux()
    OfflineMux.HandleFunc("/start", StartOffline)
    OfflineMux.HandleFunc("/end", GetLastOffset)
    http.ListenAndServe(":9091", OfflineMux)
}

func main() {
	Read("json.txt")
    InitBroker()
    InitConsumer("kafka-conn-go", 0, kafka.StartOffsetNewest)
    ListenOfflineSignal()
    	//	Write("json.txt")
	//	Append("json.txt", "wds", "wafYnother", Partition{1, 2, 3, 4})
    //	Delete("json.txt", "vds", "vdsYnother")
}
