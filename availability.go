package watchback

import (
	"sync/atomic"
	"time"
)

const maxAvailabilityDataAtomicOperationAttempt = 8

func extendTimeBoundary(d time.Duration, p * int64) {
	t := time.Now().Add(d).UnixNano()
	for attempt := 0; attempt < maxAvailabilityDataAtomicOperationAttempt; attempt++ {
		prevT := atomic.LoadInt64(p)
		if t > prevT {
			if atomic.CompareAndSwapInt64(p, prevT, t) {
				return
			}
		}
	}
}

type availabilityLogic struct {
	availableUntil int64
	blackoutUntil int64
}

func newAvailabilityLogic() (l availabilityLogic) {
	return availabilityLogic{
		availableUntil: 0,
		blackoutUntil: 0,
	}
}

func (a * availabilityLogic) Reset() {
	atomic.StoreInt64(&a.availableUntil, 0)
	atomic.StoreInt64(&a.blackoutUntil, 0)
}

func (a * availabilityLogic) ResetWithAvailableWithin(d time.Duration) {
	t := time.Now().Add(d).UnixNano()
	atomic.StoreInt64(&a.availableUntil, t)
	atomic.StoreInt64(&a.blackoutUntil, 0)
}

func (a * availabilityLogic) AvailableWithin(d time.Duration) {
	extendTimeBoundary(d, &a.availableUntil)
}

func (a * availabilityLogic) BlackoutWithin(d time.Duration) {
	extendTimeBoundary(d, &a.blackoutUntil)
}

func (a * availabilityLogic) Availability() (available bool) {
	c := time.Now().UnixNano()
	availableUtil := atomic.LoadInt64(&a.availableUntil)
	blackoutUtil := atomic.LoadInt64(&a.blackoutUntil)
	if (c > blackoutUtil) && (c <= availableUtil) {
		return true
	}
	return false
}