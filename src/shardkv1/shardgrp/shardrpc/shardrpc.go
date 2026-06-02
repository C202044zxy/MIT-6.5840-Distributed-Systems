package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	State    []byte
	Sessions []byte // serialized client dedup table (for shard migration)
	Num      shardcfg.Tnum
	Err      rpc.Err
}

type InstallShardArgs struct {
	Shard    shardcfg.Tshid
	State    []byte
	Sessions []byte // serialized client dedup table (for shard migration)
	Num      shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
