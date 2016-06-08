package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
)

func main() {
	var (
		listenHeka  = flag.String("l", ":50569", "Address to listen for heka protobuf messages on")
		listenUDP   = flag.Bool("u", true, "Listen on UDP for heka messages")
		listenTCP   = flag.Bool("t", true, "Listen on TCP for heka messages")
		listenHTTP  = flag.String("h", ":9137", "Address to expose prometheus metrics on")
		configFile  = flag.String("c", "metrics.json", "Path to metrics config")
		enablePprof = flag.Bool("pprof", false, "Enable pprof endpoint on /debug/pprof")
	)
	flag.Parse()
	if !(*listenUDP || *listenTCP) {
		log.Fatal("Can't disable both UDP and TCP listeners")
	}
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
	log.Println("Listening for heka messages on", *listenHeka)
	if *listenUDP {
		addr, err := net.ResolveUDPAddr("udp", *listenHeka)
		if err != nil {
			log.Fatal(err)
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			log.Fatal(err)
		}
		go bridge.Process(conn)
	}
	if *listenTCP {
		listener, err := net.Listen("tcp", *listenHeka)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Println(err)
					continue
				}
				log.Println("New connection from", conn.RemoteAddr())
				go bridge.Process(conn)
			}
		}()
	}
	log.Println("Listening for prometheus scrapes on", *listenHTTP+"/metrics")
	log.Fatal(http.ListenAndServe(*listenHTTP, mux))
}
