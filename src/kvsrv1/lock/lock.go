package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck 					kvtest.IKVClerk
	// You may add code here
	ls 					string
	identifier 	string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, ls: l, identifier: kvtest.RandValue(8)}
	// You may add code here

	// initialize the lock state to unlock
	for {
		value, _, err := lk.ck.Get(lk.ls)
		if err == rpc.OK && value == "unlock" {
			break
		}
		lk.ck.Put(lk.ls, "unlock", 0)
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.ls)
		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if value == "unlock" {
			err2 := lk.ck.Put(lk.ls, lk.identifier, version)
			if err2 == rpc.OK {
				return
			}
		}
		if value == lk.identifier {
			// already locked by the current client
			// but the reply is dropped
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.ls)
		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if value == lk.identifier {
			err2 := lk.ck.Put(lk.ls, "unlock", version)
			if err2 == rpc.OK {
				return
			}
		} else {
			// the lock is not held by the current client
			// or the lock is already released, but the reply is dropped
			return 
		}
		time.Sleep(100 * time.Millisecond)
	}
}
