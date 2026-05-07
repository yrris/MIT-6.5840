package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// Lock state lives in the KV server: the value of `key` is either ""
// (free) or the random ID `me` of the holder.
type Lock struct {
	ck  kvtest.IKVClerk
	key string
	me  string
}

// MakeLock creates a Lock that uses ck for storage and lockname as the
// key. Each Lock gets a fresh per-instance ID so retried Puts can be
// disambiguated under unreliable networks.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	return &Lock{ck: ck, key: lockname, me: kvtest.RandValue(8)}
}

func (lk *Lock) Acquire() {
	for {
		v, ver, err := lk.ck.Get(lk.key)
		switch err {
		case rpc.ErrNoKey:
			// First time touching this lock; create it with version 0.
			if lk.tryClaim(0) {
				return
			}
		case rpc.OK:
			if v == lk.me {
				return // a previous ErrMaybe Put actually landed
			}
			if v == "" {
				if lk.tryClaim(ver) {
					return
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// tryClaim writes `me` at the given version; returns true iff we now
// hold the lock. ErrMaybe is resolved by re-reading the key.
func (lk *Lock) tryClaim(ver rpc.Tversion) bool {
	switch lk.ck.Put(lk.key, lk.me, ver) {
	case rpc.OK:
		return true
	case rpc.ErrMaybe:
		v, _, err := lk.ck.Get(lk.key)
		return err == rpc.OK && v == lk.me
	default:
		return false
	}
}

func (lk *Lock) Release() {
	for {
		v, ver, err := lk.ck.Get(lk.key)
		if err != rpc.OK || v != lk.me {
			return
		}
		switch lk.ck.Put(lk.key, "", ver) {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			v2, _, err2 := lk.ck.Get(lk.key)
			if err2 == rpc.OK && v2 != lk.me {
				return // our reset won
			}
		}
	}
}
