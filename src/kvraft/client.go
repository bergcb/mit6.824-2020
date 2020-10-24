package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int
	requestId int
	clientId  int64

	mu sync.Mutex // Lock to protect shared access to this peer's state
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//
//func (ck *Clerk)gerNextlLeaderId() int  {
//
//	rand.
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, len(ck.servers))
//
//}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) setNextLeaderId() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

func (ck *Clerk) Get(key string) string {
	requestId := ck.genRequestId()

	for {
		ck.mu.Lock()
		var reply = GetReply{}
		args := GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		leaderId := ck.leaderId
		ck.mu.Unlock()
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrDuplicateReq) {
			return reply.Value
		} else {
			ck.setNextLeaderId()
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) genRequestId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId++
	return ck.requestId
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := ck.genRequestId()
	for {
		ck.mu.Lock()
		var reply = PutAppendReply{}
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestId: requestId,
			ClientId:  ck.clientId,
		}
		leaderId := ck.leaderId
		ck.mu.Unlock()

		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrDuplicateReq) {
			return
		} else {
			ck.setNextLeaderId()
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
