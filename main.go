package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

var (
	listenHeka  = flag.String("l", ":50569", "Address to listen for heka protobuf messages on")
	listenHTTP  = flag.String("h", ":9137", "Address to expose prometheus metrics on")
	configFile  = flag.String("c", "metrics.json", "Path to metrics config")
	enablePprof = flag.Bool("pprof", false, "Enable pprof endpoint on /debug/pprof")
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
	flag.Parse()
	mux := http.NewServeMux()
	if *enablePprof {
		for p, f := range map[string]http.HandlerFunc{
			"/debug/pprof/":        pprof.Index,
			"/debug/pprof/cmdline": pprof.Cmdline,
			"/debug/pprof/profile": pprof.Profile,
			"/debug/pprof/symbol":  pprof.Symbol,
			"/debug/pprof/trace":   pprof.Trace,
		} {

			mux.HandleFunc(p, f)
		}
	}
	bridge, err := newBridge(mux, *configFile)
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
	log.Println("Listening for udp heka messages on", *listenHeka)
	sr, err := makeSplitterRunner()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening for prometheus scrapes on", *listenHTTP+"/metrics")
	go func() { log.Fatal(http.ListenAndServe(*listenHTTP, mux)) }()
	msg := new(message.Message)
	for {
		n, record, err := sr.GetRecordFromStream(listener)
		if n > 0 && n != len(record) {
			fmt.Fprintf(os.Stderr, "Corruption detected at bytes: %d\n", n-len(record))
			continue
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
