package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type      string
	Key       string
	Value     string
	RequestId int
	ClientId  int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Notification struct {
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu                 sync.Mutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	dead               int32 // set by Kill()
	log                map[int]bool
	persister          *raft.Persister
	storage            map[string]string
	dispatcher         map[int]chan Notification
	waitTime           time.Duration
	lastApplyRequestId map[int64]int
	maxraftstate       int // snapshot if log grows this big

	// Your definitions here.
}

//每个请求都有一个channel, waitApply会从这个channel中获得apply后的结果，
func (kv *KVServer) callRaftAndStorage(op Op) Err {
	var err Err
	err = ErrWrongLeader

	kv.mu.Lock()
	if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
		err = ErrDuplicateReq
		kv.mu.Unlock()
		return err
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return ErrWrongLeader
	}
	kv.mu.Lock()
	kv.dispatcher[index] = make(chan Notification, 1)
	ch := kv.dispatcher[index]
	kv.mu.Unlock()

	select {
	case notify := <-ch:
		if notify.RequestId != op.RequestId || notify.ClientId != op.ClientId {
			err = ErrWrongLeader
		} else {
			err = OK
		}
	case <-time.After(kv.waitTime):
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
			err = ErrDuplicateReq
		} else {
			err = ErrTimeout
		}
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	delete(kv.dispatcher, index)
	kv.mu.Unlock()
	return err
}

//在kv.lastApplyRequestId中记录的key ,value相等的请求，是DuplicateRequest
//场景：网络问题，导致同一个请求重复达到server
func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int) bool {
	lastApplyRequestId, ok := kv.lastApplyRequestId[clientId]
	if ok == false || lastApplyRequestId < requestId {
		return false
	}
	return true
}

func (kv *KVServer) dataGet(key string) (err Err, val string) {
	if v, ok := kv.storage[key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		Type:      "Get",
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	err := kv.callRaftAndStorage(op)
	reply.Err = err

	if err == OK || err == ErrDuplicateReq {
		kv.mu.Lock()
		err, value := kv.dataGet(args.Key)
		reply.Value = value
		reply.Err = err
		kv.mu.Unlock()
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Type:      args.Op,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	err := kv.callRaftAndStorage(op)
	reply.Err = err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) genSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.storage)
	e.Encode(kv.lastApplyRequestId)
	kv.mu.Unlock()

	data := w.Bytes()
	return data
}

func (kv *KVServer) saveSnapshot(lastSnapshotIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	snapshot := kv.genSnapshot()
	go kv.rf.SaveSnapshot(snapshot, lastSnapshotIndex)
}

func (kv *KVServer) readSnapshotPersist(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var lastApplyRequestId map[int64]int
	if d.Decode(&storage) != nil || d.Decode(&lastApplyRequestId) != nil {
	} else {
		kv.storage = storage
		kv.lastApplyRequestId = lastApplyRequestId
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.waitTime = 500 * time.Millisecond
	kv.storage = make(map[string]string)
	kv.lastApplyRequestId = make(map[int64]int)
	kv.dispatcher = make(map[int]chan Notification)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshotPersist(kv.persister.ReadSnapshot())

	go func() {
		for applyMsg := range kv.applyCh {
			if applyMsg.CommandValid == false && applyMsg.Command.(string) == "InstallSnapshot" {
				data := applyMsg.CommandData
				kv.readSnapshotPersist(data)
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
				kv.mu.Unlock()
				continue
			}
			if op.Type == "Append" {
				kv.storage[op.Key] += op.Value
			} else if op.Type == "Put" {
				kv.storage[op.Key] = op.Value
			}
			kv.lastApplyRequestId[op.ClientId] = op.RequestId

			//只有leader或者认为自己是leader且接受了client的请求的server，才会在callRaftAndStorage里等待channel里的notify
			if ch, ok := kv.dispatcher[applyMsg.CommandIndex]; ok {
				notify := Notification{
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
				}
				ch <- notify
			}
			kv.mu.Unlock()
			kv.saveSnapshot(applyMsg.CommandIndex)
		}
	}()
	// You may need initialization code here.
	return kv
}
