package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

// 定义一个名为 Clerk 的结构体，表示客户端
type Clerk struct {
	servers    []*labrpc.ClientEnd // 存储服务器端点的数组
	cid        int64              // 客户端唯一标识符
	nextSeq    int                // 下一个请求的序号
	prevLeader int                // 上一个成功的领袖服务器的索引
}

// 生成一个随机的 int64 数字
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建并返回一个新的 Clerk 客户端实例
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()   // 生成随机的客户端唯一标识符
	ck.nextSeq = 1     // 初始化下一个请求的序号
	ck.prevLeader = 0  // 初始化上一个成功的Leader服务器的索引
	return ck
}

// 获取指定键的当前值
// 如果键不存在，返回空字符串 ""
// 在其他错误时，会一直重试
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("Client: GET(%v) [%v] starts", key, ck.cid)

	args := GetArgs{
		Key: key,
		Cid: ck.cid,
		Seq: ck.nextSeq,
	}
	ck.nextSeq++

	// 不断尝试不同服务器，直到成功或遇到错误
	for i := ck.prevLeader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.prevLeader = i
				DPrintf("Client: GET(%v) [%v] done -> %v.", key, ck.cid, reply.Value)
				return reply.Value
			case ErrNoKey:
				ck.prevLeader = i
				DPrintf("Client: GET(%v) [%v] done -> %v.", key, ck.cid, "")
				return ""
			case ErrWrongLeader:
				continue
			}
		}
	}

}

// 用于执行 Put 和 Append 操作的共享函数
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("Client: %v(%v, %v) [%v] starts", op, key, value, ck.cid)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Cid:   ck.cid,
		Seq:   ck.nextSeq,
	}
	ck.nextSeq++

	for i := ck.prevLeader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.prevLeader = i
				DPrintf("Client: %v(%v, %v) [%v] done", op, key, value, ck.cid)
				return
			case ErrWrongLeader:
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
