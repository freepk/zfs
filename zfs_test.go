package zfs

import (
	"io"
	"sync"
	"testing"
)

const (
	defaultPool = "tank"
)

func TestCreateDestroy(t *testing.T) {
	name := defaultPool + "/createDestroyTest"
	if err := Create(name); err != nil {
		t.Fail()
	}
	if err := Destroy(name, false, false); err != nil {
		t.Fail()
	}
}

func TestSnapshot(t *testing.T) {
	name := defaultPool + "/snapshotTest"
	snap0 := "snap0"
	snap1 := "snap1"
	if err := Create(name); err != nil {
		t.Fatal(err)
	}
	if err := Snapshot(name + "@" + snap0); err != nil {
		t.Fatal(err)
	}
	if err := Snapshot(name + "@" + snap1); err != nil {
		t.Fatal(err)
	}
	if err := Destroy(name, true, false); err != nil {
		t.Fatal(err)
	}
}

func TestSendRecv(t *testing.T) {
	srcName := defaultPool + "/src"
	dstName := defaultPool + "/dst"
	snapFull := "full"
	snap0 := "snap0"
	snap1 := "snap1"

	if err := Create(srcName); err != nil {
		t.Fatal(err)
	}
	if err := Create(dstName); err != nil {
		t.Fatal(err)
	}

	if err := Snapshot(srcName + "@" + snapFull); err != nil {
		t.Fatal(err)
	}

	g := new(sync.WaitGroup)
	g.Add(2)
	r, w := io.Pipe()
	go func() {
		defer r.Close()
		defer g.Done()
		if err := Recv(dstName, true, r); err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		defer w.Close()
		defer g.Done()
		if err := Send((srcName + "@" + snapFull), w); err != nil {
			t.Fatal(err)
		}
	}()
	g.Wait()

	if err := Snapshot(srcName + "@" + snap0); err != nil {
		t.Fatal(err)
	}
	if err := Snapshot(srcName + "@" + snap1); err != nil {
		t.Fatal(err)
	}
	r, w = io.Pipe()
	g.Add(2)
	go func() {
		defer r.Close()
		defer g.Done()
		if err := Recv(dstName, true, r); err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		defer w.Close()
		defer g.Done()
		name0 := srcName + "@" + snapFull
		name1 := srcName + "@" + snap1
		if err := SendDelta(name0, name1, true, w); err != nil {
			t.Fatal(err)
		}
	}()
	g.Wait()

	if err := Destroy(dstName, true, false); err != nil {
		t.Fatal(err)
	}
	if err := Destroy(srcName, true, false); err != nil {
		t.Fatal(err)
	}
}
