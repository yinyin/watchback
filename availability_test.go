package watchback

import (
	"testing"
	"time"
)

func TestAvailabilityLogic_Availability_available1(t *testing.T) {
	l := newAvailabilityLogic()
	l.AvailableWithin(time.Second * 1)
	if !l.Availability() {
		t.Errorf("expect logic available.")
	}
	time.Sleep(time.Second * 2)
	if l.Availability() {
		t.Errorf("expect logic not available.")
	}
}

func TestAvailabilityLogic_Availability_blackout1(t *testing.T) {
	l := newAvailabilityLogic()
	l.AvailableWithin(time.Second * 3)
	if !l.Availability() {
		t.Errorf("expect logic available.")
	}
	l.BlackoutWithin(time.Second * 1)
	if l.Availability() {
		t.Errorf("expect logic blackouted.")
	}
	time.Sleep(time.Second * 2)
	if !l.Availability() {
		t.Errorf("expect logic available after blackout.")
	}
	time.Sleep(time.Second * 2)
	if l.Availability() {
		t.Errorf("expect logic not available.")
	}
}

func TestAvailabilityLogic_Availability_blackout2(t *testing.T) {
	l := newAvailabilityLogic()
	l.AvailableWithin(time.Second * 3)
	l.BlackoutWithin(time.Second * 1)
	l.AvailableWithin(time.Second * 1)
	if l.Availability() {
		t.Errorf("expect logic blackouted even with available-within followed.")
	}
	time.Sleep(time.Second * 2)
	if !l.Availability() {
		t.Errorf("expect logic available after blackout.")
	}
}

func TestAvailabilityLogic_Reset(t *testing.T) {
	l := newAvailabilityLogic()
	if l.Availability() {
		t.Errorf("expect logic not available (new).")
	}
	l.AvailableWithin(time.Second * 3)
	if !l.Availability() {
		t.Errorf("expect logic available (available-within).")
	}
	l.Reset()
	if l.Availability() {
		t.Errorf("expect logic not available (reset).")
	}
	l.AvailableWithin(time.Second * 3)
	l.BlackoutWithin(time.Second * 3)
	l.Reset()
	l.AvailableWithin(time.Second * 1)
	if !l.Availability() {
		t.Errorf("expect logic available (reset, available-within).")
	}
}

func TestAvailabilityLogic_ResetWithAvailableWithin(t *testing.T) {
	l := newAvailabilityLogic()
	if l.Availability() {
		t.Errorf("expect logic not available (new).")
	}
	l.AvailableWithin(time.Second * 5)
	l.BlackoutWithin(time.Second * 3)
	if l.Availability() {
		t.Errorf("expect logic not available (reset).")
	}
	l.ResetWithAvailableWithin(time.Second * 1)
	if !l.Availability() {
		t.Errorf("expect logic available (reset-with-available-within).")
	}
	time.Sleep(time.Second * 2)
	if l.Availability() {
		t.Errorf("expect logic not available (expired).")
	}
}
