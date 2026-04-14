package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"reflect"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	old := sck.Query()
	newc := sck.QueryNew()
	if newc.Num > old.Num {
		sck.ChangeConfigTo(newc.Copy())
	}
}

func configKVEqual(a, b string) bool {
	if a == b {
		return true
	}
	ca, cb := shardcfg.FromString(a), shardcfg.FromString(b)
	if ca.Num != cb.Num {
		return false
	}
	if ca.Shards != cb.Shards {
		return false
	}
	return reflect.DeepEqual(ca.Groups, cb.Groups)
}

// putConfigKey writes value with CAS expectVer; handles ErrMaybe/ErrVersion when the
// write may have succeeded but the clerk could not confirm (kvsrv1 clerk semantics).
func (sck *ShardCtrler) putConfigKey(key, want string, expectVer rpc.Tversion) bool {
	for {
		err := sck.IKVClerk.Put(key, want, expectVer)
		if err == rpc.OK {
			return true
		}
		if err == rpc.ErrVersion || err == rpc.ErrMaybe {
			got, _, e := sck.IKVClerk.Get(key)
			if e == rpc.OK && configKVEqual(want, got) {
				return true
			}
			if e == rpc.OK {
				return false
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	s := cfg.String()
	sck.putConfigKey("Config", s, 0)
	sck.putConfigKey("NewConfig", s, 0)

	// checks whether there is a uncommitted configuration
	old := sck.Query()
	new := sck.QueryNew()
	if new.Num > old.Num {
		sck.ChangeConfigTo(new.Copy())
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	old := sck.Query()
	// first write the new config
	new.Num = old.Num + 1
	newStr := new.String()
	ok := sck.putConfigKey("NewConfig", newStr, rpc.Tversion(old.Num))
	if !ok {
		return
	}

	for shard := range old.Shards {
		oldGid := old.Shards[shard]
		newGid := new.Shards[shard]
		if oldGid != newGid && oldGid != 0 && newGid != 0 {
			srcClerk := shardgrp.MakeClerk(sck.clnt, old.Groups[oldGid])
			dstClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[newGid])
			for {
				state, err := srcClerk.FreezeShard(shardcfg.Tshid(shard), old.Num)
				if err != rpc.OK {
					continue
				}
				err = dstClerk.InstallShard(shardcfg.Tshid(shard), state, old.Num)
				if err != rpc.OK {
					continue
				}
				err = srcClerk.DeleteShard(shardcfg.Tshid(shard), old.Num)
				if err != rpc.OK {
					continue
				}
				break
			}
		}
	}

	// write it to original config. commit the whole process.
	sck.putConfigKey("Config", newStr, rpc.Tversion(old.Num))
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	for {
		cfg_str, _, err := sck.IKVClerk.Get("Config")
		if err == rpc.OK {
			return shardcfg.FromString(cfg_str)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (sck *ShardCtrler) QueryNew() *shardcfg.ShardConfig {
	// Your code here.
	for {
		cfg_str, _, err := sck.IKVClerk.Get("NewConfig")
		if err == rpc.OK {
			return shardcfg.FromString(cfg_str)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
