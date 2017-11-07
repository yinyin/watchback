package watchback

import (
	"testing"
	"time"
)

func runAndWaitStateTransporterForTesting(t *testing.T, s *stateTransporter, h stateHandler) (waitStop func()) {
	loopStopped := make(chan int32, 1)
	go func() {
		s.RunStateTransportLoop(h)
		loopStopped <- 1
	}()
	return func() {
		select {
		case <-loopStopped:
		case <-time.After(time.Second * 3):
			t.Errorf("state transporter loop does not stop within 3 seconds")
		}
	}
}

type mockStateShift1 struct {
	s0 time.Time
	i1 time.Time
	s1 time.Time
	s2 time.Time
	c2 chan time.Time
	s3 time.Time
	c3 chan time.Time
}

func newMockStateShift1() (s *mockStateShift1) {
	return &mockStateShift1{
		s0: time.Now(),
		c2: make(chan time.Time, 1),
		c3: make(chan time.Time, 1),
	}
}

func (m *mockStateShift1) h1() (next stateHandler, after time.Duration) {
	m.s1 = time.Now()
	return m.h2, 0
}

func (m *mockStateShift1) h2() (next stateHandler, after time.Duration) {
	m.c2 <- time.Now()
	m.s2 = time.Now()
	return m.h3, time.Second*2 + time.Microsecond*5
}

func (m *mockStateShift1) h3() (next stateHandler, after time.Duration) {
	m.s3 = time.Now()
	m.c3 <- time.Now()
	return nil, 0
}

func (m *mockStateShift1) detour1() {
	m.i1 = time.Now()
}

func (m *mockStateShift1) validateT1normal(t *testing.T) {
	if m.s0.After(m.s1) {
		t.Errorf("expecting s0 < s1: %v, %v", m.s0, m.s1)
	}
	if m.s1.After(m.s2) || m.s0.After(m.s2) {
		t.Errorf("expecting s0 < s1 < s2: %v, %v, %v", m.s0, m.s1, m.s2)
	}
	d := m.s3.Sub(m.s2)
	if m.s2.After(m.s3) || m.s0.After(m.s3) || (d < time.Second*2) {
		t.Errorf("expecting s0 < s2 < s3 and s3 - s2 > 2sec: %v, %v, %v, %v", m.s0, m.s2, m.s3, d)
	}
}

func (m *mockStateShift1) testT1normal(t *testing.T, s *stateTransporter) {
	waitStop := runAndWaitStateTransporterForTesting(t, s, m.h1)
	<-m.c3
	m.validateT1normal(t)
	waitStop()
	if nil == s.AppendDetour(nil) {
		t.Errorf("expecting failure on AppendDetour after transport stopped")
	}
}

func (m *mockStateShift1) validateT2detour(t *testing.T) {
	if m.s0.After(m.s1) {
		t.Errorf("expecting s0 < s1: %v, %v", m.s0, m.s1)
	}
	if m.s1.After(m.s2) || m.s0.After(m.s2) {
		t.Errorf("expecting s0 < s1 < s2: %v, %v, %v", m.s0, m.s1, m.s2)
	}
	if m.s2.After(m.i1) || m.s0.After(m.i1) {
		t.Errorf("expecting s0 < s2 < i1: %v, %v, %v", m.s0, m.s2, m.i1)
	}
	if m.i1.After(m.s3) || m.s0.After(m.s3) {
		t.Errorf("expecting s0 < i1 < s3: %v, %v, %v", m.s0, m.i1, m.s3)
	}
}

func (m *mockStateShift1) testT2detour(t *testing.T, s *stateTransporter) {
	waitStop := runAndWaitStateTransporterForTesting(t, s, m.h1)
	<-m.c2
	s.AppendDetour(m.detour1)
	<-m.c3
	m.validateT2detour(t)
	waitStop()
	if nil == s.AppendDetour(nil) {
		t.Errorf("expecting failure on AppendDetour after transport stopped")
	}
}

func (m *mockStateShift1) testT3stop(t *testing.T, s *stateTransporter) {
	waitStop := runAndWaitStateTransporterForTesting(t, s, m.h1)
	s.Stop()
	waitStop()
	if nil == s.AppendDetour(nil) {
		t.Errorf("expecting failure on AppendDetour after transport stopped")
	}
}

func TestStateTransporter_RunStateTransportLoop_shift1normal(t *testing.T) {
	s := newStateTransporter()
	newMockStateShift1().testT1normal(t, &s)
}

func TestStateTransporter_RunStateTransportLoop_shift1detour(t *testing.T) {
	s := newStateTransporter()
	newMockStateShift1().testT2detour(t, &s)
}

func TestStateTransporter_RunStateTransportLoop_shift1stop(t *testing.T) {
	s := newStateTransporter()
	newMockStateShift1().testT3stop(t, &s)
}
