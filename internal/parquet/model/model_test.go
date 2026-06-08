package model

import (
	"testing"
	"time"
)

func TestStructureEventMapsTraffic(t *testing.T) {
	start := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	ev := EventJSON{
		NodeID: "n1",
		Start:  start,
		End:    end,
		VirtualTraffic: []ConnectionCountsJSON{
			{Proto: 6, Src: "a:1", Dst: "b:2", TxPackets: 1, TxBytes: 2, RxPackets: 3, RxBytes: 4},
			{Proto: 17, Src: "c:3", Dst: "d:4", TxPackets: 5, TxBytes: 6, RxPackets: 7, RxBytes: 8},
		},
	}

	row := StructureEvent(ev)

	if row.NodeID != "n1" {
		t.Errorf("NodeID = %q", row.NodeID)
	}
	if row.Start != start.Format(time.RFC3339) {
		t.Errorf("Start = %q, want %q", row.Start, start.Format(time.RFC3339))
	}
	if row.End != end.Format(time.RFC3339) {
		t.Errorf("End = %q, want %q", row.End, end.Format(time.RFC3339))
	}
	if row.Logtail == nil {
		t.Error("Logtail should be non-nil")
	}
	if row.VirtualTraffic == nil {
		t.Fatal("VirtualTraffic should be non-nil")
	}
	// Representative proto/src/dst come from the first connection.
	if row.VirtualTraffic.Proto != 6 || row.VirtualTraffic.Src != "a:1" || row.VirtualTraffic.Dst != "b:2" {
		t.Errorf("representative = %+v", row.VirtualTraffic)
	}
	if len(row.VirtualTraffic.Conns) != 2 {
		t.Fatalf("Conns = %d, want 2", len(row.VirtualTraffic.Conns))
	}
	c := row.VirtualTraffic.Conns[0]
	if c.Proto != 6 || c.TxPackets != 1 || c.TxBytes != 2 || c.RxPackets != 3 || c.RxBytes != 4 {
		t.Errorf("conn[0] = %+v", c)
	}
	// Empty categories must be nil so parquet stores them as null groups.
	if row.SubnetTraffic != nil || row.ExitTraffic != nil || row.PhysicalTraffic != nil {
		t.Error("empty traffic categories should be nil")
	}
}

func TestStructureEventNoTraffic(t *testing.T) {
	row := StructureEvent(EventJSON{NodeID: "n2"})
	if row.NodeID != "n2" {
		t.Errorf("NodeID = %q", row.NodeID)
	}
	if row.VirtualTraffic != nil || row.SubnetTraffic != nil || row.ExitTraffic != nil || row.PhysicalTraffic != nil {
		t.Error("no traffic should yield all-nil traffic structs")
	}
}
