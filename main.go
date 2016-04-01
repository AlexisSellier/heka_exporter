package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

var (
	listenHeka = flag.String("l", "0.0.0.0:50569", "Address to listen for heka protobuf messages on")
	listenHTTP = flag.String("h", "0.0.0.0:8050", "Address to expose prometheus metrics on")
	configFile = flag.String("c", "metrics.json", "Path to metrics config")
)

// from https://github.com/mozilla-services/heka/blob/dev/cmd/heka-cat/main.go
func makeSplitterRunner() (pipeline.SplitterRunner, error) {
	splitter := &pipeline.HekaFramingSplitter{}
	config := splitter.ConfigStruct()
	err := splitter.Init(config)
	if err != nil {
		return nil, fmt.Errorf("Error initializing HekaFramingSplitter: %s", err)
	}
	srConfig := pipeline.CommonSplitterConfig{}
	sRunner := pipeline.NewSplitterRunner("HekaFramingSplitter", splitter, srConfig)
	return sRunner, nil
}

func main() {
	bridge, err := newBridge(*configFile)
	if err != nil {
		log.Fatalf("Couldn't read config %s: %s", *configFile, err)
	}

	addr, err := net.ResolveUDPAddr("udp", *listenHeka)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	sr, err := makeSplitterRunner()
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(http.ListenAndServe(*listenHTTP, nil)) }()
	msg := new(message.Message)
	for {
		n, record, err := sr.GetRecordFromStream(listener)
		if n > 0 && n != len(record) {
			fmt.Fprintf(os.Stderr, "Corruption detected at bytes: %d\n", n-len(record))
		}
		if err != nil {
			log.Fatal(err)
		}
		headerLen := int(record[1]) + message.HEADER_FRAMING_SIZE
		if err = proto.Unmarshal(record[headerLen:], msg); err != nil {
			log.Println(err)
			continue
		}
		if err := bridge.Process(msg); err != nil {
			log.Println(err)
		}
	}
}
