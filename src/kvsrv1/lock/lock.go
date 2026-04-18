package lock

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	uid     string
	lockKey string
	lck     sync.Mutex
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.uid = kvtest.RandValue(8)
	lk.lockKey = l
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || val == "FREE" || val == lk.uid {
			errr := lk.ck.Put(lk.lockKey, lk.uid, ver)
			if errr == rpc.ErrVersion {
				continue
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			return
		}
		if val != lk.uid {
			return
		}

		errr := lk.ck.Put(lk.lockKey, "FREE", ver)

		if errr == rpc.OK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
