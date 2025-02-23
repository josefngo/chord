package main

import (
	"fmt"
	"io"
	"math/big"
	"net/rpc/jsonrpc"
	"os"
)

type NotifyRPCReply struct {
}

type GetPredecessorRPCReply struct {
	Address string
}

type FileRPC struct {
	Id      *big.Int
	Name    string
	Content []byte
}

type SaveFileRPCReply struct {
}

func (node *Node) stabilize() {
	// Check if successor has a predecessor
	var predecessor GetPredecessorRPCReply

	err := RPCCall(node.Successor, "Node.GetPredecessorRPC", "", &predecessor)

	if err != nil {
		fmt.Println("Get predecessor fail for", node.Successor, err)
		node.Successor = node.Address
		predecessor.Address = ""
	}

	if predecessor.Address != "" {
		var successorID GetIdentifierRPCReply
		err = RPCCall(node.Successor, "Node.GetIdentifierRPC", "", &successorID)
		if err != nil {
			fmt.Print("Failed to get ID", err)
		}
		var predecessorID GetIdentifierRPCReply
		err = RPCCall(predecessor.Address, "Node.GetIdentifierRPC", "", &predecessorID)
		if err != nil {
			fmt.Print("Failed to get ID", err)
		}

		if between(node.Identifier, predecessorID.ID, successorID.ID, false) {
			node.Successor = predecessor.Address
		}

	}

	err = RPCCall(node.Successor, "Node.NotifyRPC", node.Address, &NotifyRPCReply{})
	if err != nil {
		fmt.Println("Failed notify", err)
	}
}

func (node *Node) fixFingers() {

	node.NextFinger = node.NextFinger + 1
	if node.NextFinger > len(node.FingerTable)-1 {
		node.NextFinger = 1
	}

	finger := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(node.NextFinger)-1), nil)
	res := new(big.Int).Add(node.Identifier, finger)

	resultID := new(big.Int).Mod(res, hashMod)

	var reply FindSuccessorRPCReply
	err := node.FindSuccessorRPC(resultID, &reply)
	if err != nil {
		fmt.Println("Failed to find successor", err)
	}
	if reply.Found {

		var fingerID GetIdentifierRPCReply
		err = RPCCall(reply.SuccessorAddress, "Node.GetIdentifierRPC", "", &fingerID)
		if err != nil {
			fmt.Print("Failed to get ID", err)
		}

		node.FingerTable[node.NextFinger].Identifier = resultID
		if node.FingerTable[node.NextFinger].Address != reply.SuccessorAddress && reply.SuccessorAddress != "" {
			node.FingerTable[node.NextFinger].Address = reply.SuccessorAddress
		}
	}
}

func (node *Node) checkPredecessors() {

	predecessor := node.Predecessor
	if predecessor != "" {
		_, err := jsonrpc.Dial("tcp", predecessor)
		if err != nil {
			fmt.Println("Predecessor has failed: ", predecessor)
			node.Predecessor = ""
		}
	}
}

func (node *Node) NotifyRPC(address string, reply *NotifyRPCReply) error {
	if node.Successor != node.Address {
		node.MoveFiles(address)
	}

	// Check if predecessor exists
	if node.Predecessor == "" {
		node.Predecessor = address
		return nil
	}

	var addressID GetIdentifierRPCReply
	err := RPCCall(address, "Node.GetIdentifierRPC", "", &addressID)
	if err != nil {
		return err
	}

	var predecessorID GetIdentifierRPCReply
	err = RPCCall(node.Predecessor, "Node.GetIdentifierRPC", "", &predecessorID)
	if err != nil {
		return err
	}

	// Check if new address is between current predecessor and self
	if between(predecessorID.ID, addressID.ID, node.Identifier, false) {
		node.Predecessor = address
		return nil
	}

	return nil
}

func (node *Node) MoveFiles(address string) {

	var getID GetIdentifierRPCReply
	err := RPCCall(address, "Node.GetIdentifierRPC", "", &getID)
	if err != nil {
		fmt.Println("Get id failed: ", err)
		return
	}

	for fileID, fileName := range node.Bucket {
		path := "files/" + node.Identifier.String() + "/" + fileName

		file, err := os.Open(path)
		if err != nil {
			fmt.Println("Can't open file", err)
		}
		defer file.Close()

		//shouldMove := fileID.Cmp(node.Identifier) != 0 || fileID.Cmp(getID.ID) == 0

		if between(fileID, getID.ID, node.Identifier, false) && fileID.Cmp(node.Identifier) != 0 || fileID.Cmp(getID.ID) == 0 {

			fmt.Println("Moving file", fileID, "From", node.Identifier, "To", getID.ID)
			id := strHash(fileName)
			id.Mod(id, hashMod)
			fileContent, err := io.ReadAll(file)
			if err != nil {
				fmt.Println("Error reading file", err)
				return
			}

			RPCFile := FileRPC{
				Id:      id,
				Name:    fileName,
				Content: fileContent,
			}

			var aa SaveFileRPCReply
			err = RPCCall(address, "Node.SaveFileRPC", RPCFile, &aa)
			if err != nil {
				fmt.Println("Sending file failed", err)
				return
			}

			delete(node.Bucket, fileID)

			err = os.Remove(path)
			if err != nil {
				fmt.Println("Failed deleting file")
			}

		}
	}

}

func (node *Node) GetPredecessorRPC(request string, reply *GetPredecessorRPCReply) error {
	reply.Address = node.Predecessor
	return nil
}
