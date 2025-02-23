package main

import (
	"bufio"
	"crypto/sha1"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type NodeRPC struct {
	Address    string
	Identifier *big.Int
}

var (
	ipAddress             string
	port                  string
	joinAddress           string
	joinPort              int
	stabilizeTime         int
	fixFingersTime        int
	checkPredecessorsTime int
	nSuccessors           int
	identifier            string
)

func main() {
	handleFlags()
	node := InitNode(ipAddress+":"+port, identifier)

	rpc.Register(node)
	listener, err := net.Listen("tcp", ipAddress+":"+port)
	if err != nil {
		fmt.Println("TCP Listen failed: ", err.Error())
		os.Exit(1)
	}

	go listen(listener, node)

	join := (joinAddress != "" && joinPort != 0)
	if join {
		// Join the network
		node.JoinChord(joinAddress + ":" + strconv.Itoa(joinPort))
	} else {
		// Create a new network
		node.CreateChord()
	}

	// Start go routines for stab, fixFing, checkPre
	go taskTimer(stabilizeTime, func() { node.stabilize() })
	go taskTimer(fixFingersTime, func() { node.fixFingers() })
	go taskTimer(checkPredecessorsTime, func() { node.checkPredecessors() })

	inputListener(node)
	// Listen for input
}

func taskTimer(t int, task func()) {
	for {
		time.Sleep(time.Duration(t) * time.Millisecond)
		task()
	}
}

func inputListener(node *Node) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Command: ")
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "Lookup":
			fmt.Print("Key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			result := keyLookup(key, node)

			fmt.Println("Key is at address: ", result)

		case "PrintState":
			node.PrintState()

		case "StoreFile":
			fmt.Print("File: ")
			file, _ := reader.ReadString('\n')
			file = strings.TrimSpace(file)

			err := node.StoreFile(file)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("File saved")
			}
		case "Get":
			fmt.Print("File: ")
			file, _ := reader.ReadString('\n')
			file = strings.TrimSpace(file)

			err := node.GetFile(file)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("File fetched")
			}

		default:
			fmt.Println("Unknown command:", cmd)
		}

	}
}

func strHash(s string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func keyLookup(key string, node *Node) string {
	keyHash := strHash(key)
	address := find(keyHash, node.Address)
	if address == "" {
		return "Key not found"
	} else {
		return address
	}
}

func RPCCall(address string, method string, args interface{}, reply interface{}) error {
	client, err := jsonrpc.Dial("tcp", address)
	if err != nil {
		fmt.Println("Dial failed: ", err.Error())
		return err
	}
	defer client.Close()
	// fmt.Println("CALLING: ", address, "with", method)
	err = client.Call(method, args, reply)
	if err != nil {
		fmt.Println("Call failed: ", err.Error())
		return err
	}
	return nil
}

func listen(listener net.Listener, node *Node) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Listener accept failed:", err.Error())
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}

func handleFlags() {
	flag.StringVar(&ipAddress, "a", "", "The IP address to bind and advertise")
	flag.StringVar(&port, "p", "", "The port to bind and listen on")
	flag.StringVar(&joinAddress, "ja", "", "The IP address of the machine running a Chord node to join")
	flag.IntVar(&joinPort, "jp", 0, "The port of an existing Chord node to join")
	flag.IntVar(&stabilizeTime, "ts", 0, "Time between invocations of ‘stabilize’ in milliseconds")
	flag.IntVar(&fixFingersTime, "tff", 0, "Time between invocations of ‘fix fingers’ in milliseconds")
	flag.IntVar(&checkPredecessorsTime, "tcp", 0, "Time between invocations of ‘check predecessor’ in milliseconds")
	flag.IntVar(&nSuccessors, "r", 0, "Number of successors maintained by the Chord client")
	flag.StringVar(&identifier, "i", "", "The identifier (ID) assigned to the Chord client")

	flag.Parse()

	if ipAddress == "" || port == "" || stabilizeTime == 0 || fixFingersTime == 0 || checkPredecessorsTime == 0 || nSuccessors == 0 {
		fmt.Println("Error: Missing required arguments. Please provide values for all required flags.")
		flag.PrintDefaults()
		os.Exit(1)
	}

}

func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}
