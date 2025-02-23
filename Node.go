package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
)

type TableEntry struct {
	Address    string
	Identifier *big.Int
}

var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(6)), nil)

type Node struct {
	Address     string
	Identifier  *big.Int
	Successor   string
	FingerTable []TableEntry
	NextFinger  int
	Predecessor string
	Bucket      map[*big.Int]string
}

func InitNode(address string, identifier string) *Node {
	var s string
	if identifier == "" { // No identifier given
		fmt.Println("No identifier given, using address as identifier")
		s = address
	} else { // Identifier given with flag
		s = identifier
	}
	id := strHash(s)
	id.Mod(id, hashMod)

	node := &Node{
		Address:     address,
		Identifier:  id,
		Successor:   "",
		FingerTable: make([]TableEntry, 7),
		NextFinger:  0,
		Predecessor: "",
		Bucket:      make(map[*big.Int]string),
	}

	node.InitFingerTable()

	files, err := os.ReadDir("files/" + node.Identifier.String())
	if err != nil {
		fmt.Println("Directory not found: ", err)
	}

	for _, file := range files {
		name := file.Name()
		hash := strHash(name)
		hash.Mod(hash, hashMod)
		node.Bucket[hash] = name
	}

	return node
}

func (node *Node) InitFingerTable() {
	node.FingerTable[0].Identifier = node.Identifier
	for i := 0; i < len(node.FingerTable); i++ {
		node.FingerTable[i].Address = node.Address
	}
}

func (node *Node) JoinChord(address string) {
	var reply FindSuccessorRPCReply
	err := RPCCall(address, "Node.FindSuccessorRPC", node.Identifier, &reply)
	fmt.Println("Successor: ", reply.SuccessorAddress)
	node.Successor = reply.SuccessorAddress
	if err != nil {
		log.Fatal("Error joining chord: ", err)
		return
	}
	err = RPCCall(node.Successor, "Node.NotifyRPC", node.Address, &reply)
	if err != nil {
		log.Fatal("Error notifying successor: ", err)
		return
	}
}

func (node *Node) CreateChord() {

	node.Predecessor = ""

	node.Successor = node.Address

}

func StoreLocally(RPCFile *FileRPC) {

	fmt.Println(RPCFile.Name)
	file, err := os.Create(RPCFile.Name)
	if err != nil {
		fmt.Println("Storing file failed on create", err)
		return
	}
	defer file.Close()

	_, err = file.Write(RPCFile.Content)
	if err != nil {
		fmt.Println("Storing file failed on write", err)
		return
	}

}

func (node *Node) GetFileRPC(RPCFile FileRPC, reply *FileRPC) error {

	fileName := ""

	for id, name := range node.Bucket {
		if id.Cmp(RPCFile.Id) == 0 {
			fileName = name
		}
	}

	if fileName == "" {
		fmt.Println("File was not in bucket")
		return errors.New("File not found")
	}

	file, err := os.Open("files/" + node.Identifier.String() + "/" + fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	reply.Name = fileName
	reply.Content, err = io.ReadAll(file)
	if err != nil {
		return err
	}

	return nil

}

func (node *Node) GetFile(fileName string) error {
	address := find(strHash(fileName), node.Address)
	if address == "" {
		return errors.New("Failed to find address")
	}

	id := strHash(fileName)
	id.Mod(id, hashMod)

	file := FileRPC{
		Id:   id,
		Name: fileName,
	}

	fmt.Println("find address: ", address)
	err := RPCCall(address, "Node.GetFileRPC", file, &file)
	if err != nil {
		fmt.Println("Failed getting file", err)
		return err
	}

	StoreLocally(&file)

	return nil

}

func (node *Node) StoreFile(fileName string) error {

	address := find(strHash(fileName), node.Address)
	if address == "" {
		return errors.New("Failed to find address")
	}

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Failed to open file", err)
		return err
	}
	defer file.Close()
	fileContent, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file", err)
		return err
	}
	id := strHash(fileName)
	id.Mod(id, hashMod)

	RPCFile := FileRPC{
		Id:      id,
		Name:    fileName,
		Content: fileContent,
	}

	var reply SaveFileRPCReply // not used
	err = RPCCall(address, "Node.SaveFileRPC", RPCFile, &reply)
	if err != nil {
		fmt.Println("Sending file failed", err)
		return err
	}

	return nil
}

func (node *Node) SaveFileRPC(file FileRPC, reply *SaveFileRPCReply) error {

	for key, _ := range node.Bucket {

		if key.Cmp(file.Id) == 0 {
			return nil
		}
	}

	node.Bucket[file.Id] = file.Name

	path := "files/" + node.Identifier.String() + "/" + file.Name

	localFile, err := os.Create(path)
	if err != nil {
		fmt.Print("Create file failed", err)
		return err
	}
	defer localFile.Close()

	_, err = localFile.Write(file.Content)
	if err != nil {
		fmt.Println("Failed writing file", err)
		return err
	}

	return nil
}

func (node *Node) PrintState() {

	fmt.Println("Node Identifier: ", node.Identifier)
	fmt.Println("Node Address: ", node.Address)
	fmt.Println("Node Predecessor: ", node.Predecessor)
	fmt.Println("Node Successor: ", node.Successor)

	fmt.Println("Node Finger Table: ")
	for i := 1; i < len(node.FingerTable); i++ {
		ent := node.FingerTable[i]
		id := ent.Identifier
		address := ent.Address
		fmt.Println("Finger Entry: ", i, ", ID: ", id, ", Address: ", address)
	}
}

func (node *Node) indexToId(index int) *big.Int {
	id := node.Identifier
	ex := big.NewInt(int64(index) - 1)
	//2^(k-1)
	fingerEntry := new(big.Int).Exp(big.NewInt(2), ex, nil)
	// n + 2^(k-1)
	sum := new(big.Int).Add(id, fingerEntry)
	// (n + 2^(k-1) ) mod 2^m , 1 <= k <= m
	return new(big.Int).Mod(sum, hashMod)
}
