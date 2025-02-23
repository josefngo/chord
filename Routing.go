package main

import (
	"fmt"
	"math/big"
)

type FindSuccessorRPCReply struct {
	Found            bool
	SuccessorAddress string
}

type GetIdentifierRPCReply struct {
	ID *big.Int
}

func (node *Node) FindSuccessorRPC(requestID *big.Int, reply *FindSuccessorRPCReply) error {
	var getIdRPCReply GetIdentifierRPCReply
	err := RPCCall(node.Successor, "Node.GetIdentifierRPC", "", &getIdRPCReply)
	if err != nil {
		return err
	}
	if between(node.Identifier, requestID, getIdRPCReply.ID, true) {
		reply.Found = true
		reply.SuccessorAddress = node.Successor
	} else {
		successor := node.ClosestPrecedingFinger(requestID)
		var findSuccessorRPCReply FindSuccessorRPCReply
		err := RPCCall(successor, "Node.FindSuccessorRPC", requestID, &findSuccessorRPCReply)
		if err != nil {
			reply.Found = false
			return err
		} else {
			reply.Found = true
			reply.SuccessorAddress = findSuccessorRPCReply.SuccessorAddress
		}
	}
	return nil
}

func (node *Node) ClosestPrecedingFinger(requestID *big.Int) string {
	fingerTableSize := len(node.FingerTable)
	for i := fingerTableSize - 1; i >= 1; i-- {
		var reply GetIdentifierRPCReply
		err := RPCCall(node.FingerTable[i].Address, "Node.GetIdentifierRPC", "", &reply)
		if err != nil {
			fmt.Println("Error in ClosestPrecedingFinger function: ", err)
			continue
		}
		if between(node.Identifier, reply.ID, requestID, false) {
			return node.FingerTable[i].Address
		}
	}
	return node.Successor
}

func (node *Node) GetIdentifierRPC(request string, reply *GetIdentifierRPCReply) error {
	reply.ID = node.Identifier
	return nil
}

func find(id *big.Int, node string) string {
	id2 := id.Mod(id, hashMod)
	fmt.Println("Finding: ", id2)
	found := false
	nextNode := node
	i := 0
	limit := 10
	for !found && i < limit {
		reply := FindSuccessorRPCReply{}
		err := RPCCall(nextNode, "Node.FindSuccessorRPC", id2, &reply)
		if err != nil {
			fmt.Println("Error in find: ", err)
		}
		found = reply.Found
		nextNode = reply.SuccessorAddress
		fmt.Println("Next node: ", nextNode)
		i++
	}
	if found {
		fmt.Println("Find succeeded in ", i, " steps")
		return nextNode
	} else {
		fmt.Println("Find failed")
		return ""
	}
}
