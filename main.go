package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/willdot/raft/raft"
	"github.com/willdot/raft/server"
	"github.com/willdot/raft/service"
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "string slice flag"
}

func (i *arrayFlags) Set(value string) error {
	allVals := strings.Split(value, ",")

	for _, val := range allVals {
		*i = append(*i, strings.TrimSpace(val))
	}

	return nil
}

var peers arrayFlags

func main() {
	port := flag.Int("port", 0, "the port for this server")
	flag.Var(&peers, "peers", "an array of peers to connect to")

	flag.Parse()
	if *port < 1 {
		log.Fatal(fmt.Errorf("invalid flag"))
	}

	addr := fmt.Sprintf("localhost:%d", *port)
	fmt.Println(addr)

	service := service.NewService(addr)

	err := service.AddPeer(peers)
	if err != nil {
		fmt.Printf("error adding peers: %s\n", err)
	}

	raft, err := raft.NewRaft(service, time.Second, peers)
	if err != nil {
		log.Fatal(err)
	}

	server, err := server.NewRPCServer(addr, raft)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(server.Start())
}
