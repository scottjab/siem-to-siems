// Package model holds the event DTOs and parquet row schemas used by the parquet
// destination. It is a direct port of the runtime types from siem-to-parquet so that
// the parquet files this service writes are byte-for-byte schema-compatible.
package model

import "time"

// DTOs for incoming events.

type ConnectionCountsJSON struct {
	Proto     int    `json:"proto"`
	Src       string `json:"src"`
	Dst       string `json:"dst"`
	TxPackets uint64 `json:"txPkts"`
	TxBytes   uint64 `json:"txBytes"`
	RxPackets uint64 `json:"rxPkts"`
	RxBytes   uint64 `json:"rxBytes"`
}

type EventJSON struct {
	NodeID string    `json:"nodeId"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`

	SrcNode  Node   `json:"srcNode,omitzero"`
	DstNodes []Node `json:"dstNodes,omitempty"`

	VirtualTraffic  []ConnectionCountsJSON `json:"virtualTraffic"`
	SubnetTraffic   []ConnectionCountsJSON `json:"subnetTraffic"`
	ExitTraffic     []ConnectionCountsJSON `json:"exitTraffic"`
	PhysicalTraffic []ConnectionCountsJSON `json:"physicalTraffic"`
}

type Node struct {
	// NodeID is the stable ID of the node.
	NodeID string `json:"nodeId" parquet:"name=nodeId, type=BYTE_ARRAY, convertedtype=UTF8"`

	Name string `json:"name,omitzero" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`

	Addresses []string `json:"addresses,omitempty" parquet:"name=addresses, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`

	// OS is the operating system of the node.
	OS string `json:"os,omitzero" parquet:"name=os, type=BYTE_ARRAY, convertedtype=UTF8"`

	// User is the user that owns the node. Not populated if the node is tagged.
	User string `json:"user,omitzero" parquet:"name=user, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Tags are the tags of the node. Not populated if the node is owned by a user.
	Tags []string `json:"tags,omitempty" parquet:"name=tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
}

// Structured parquet row that preserves nested structure.
type ParquetStructuredNetlogRow struct {
	Logtail         *LogtailStruct `parquet:"name=logtail"`
	NodeID          string         `parquet:"name=nodeId, type=BYTE_ARRAY, convertedtype=UTF8"`
	Start           string         `parquet:"name=start, type=BYTE_ARRAY, convertedtype=UTF8"`
	End             string         `parquet:"name=end, type=BYTE_ARRAY, convertedtype=UTF8"`
	SrcNode         *Node          `parquet:"name=srcNode"`
	DstNodes        []*Node        `parquet:"name=dstNodes"`
	SubnetTraffic   *TrafficStruct `parquet:"name=subnetTraffic"`
	PhysicalTraffic *TrafficStruct `parquet:"name=physicalTraffic"`
	ExitTraffic     *TrafficStruct `parquet:"name=exitTraffic"`
	VirtualTraffic  *TrafficStruct `parquet:"name=virtualTraffic"`
	Text            string         `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// LogtailStruct is the union of all versions - includes all possible fields.
type LogtailStruct struct {
	ID         string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	ClientID   string `parquet:"name=client_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Collection string `parquet:"name=collection, type=BYTE_ARRAY, convertedtype=UTF8"`
	ClientTime string `parquet:"name=client_time, type=BYTE_ARRAY, convertedtype=UTF8"`
	ServerTime string `parquet:"name=server_time, type=BYTE_ARRAY, convertedtype=UTF8"`
	ProcID     int64  `parquet:"name=proc_id, type=INT64"`
	ProcSeq    int64  `parquet:"name=proc_seq, type=INT64"`
}

type TrafficStruct struct {
	Proto int64                    `parquet:"name=proto, type=INT64"`
	Src   string                   `parquet:"name=src, type=BYTE_ARRAY, convertedtype=UTF8"`
	Dst   string                   `parquet:"name=dst, type=BYTE_ARRAY, convertedtype=UTF8"`
	Conns []ConnectionCountsStruct `parquet:"name=conns"`
}

type ConnectionCountsStruct struct {
	Proto     int64  `parquet:"name=proto, type=INT64"`
	Src       string `parquet:"name=src, type=BYTE_ARRAY, convertedtype=UTF8"`
	Dst       string `parquet:"name=dst, type=BYTE_ARRAY, convertedtype=UTF8"`
	TxPackets int64  `parquet:"name=txPkts, type=INT64"`
	TxBytes   int64  `parquet:"name=txBytes, type=INT64"`
	RxPackets int64  `parquet:"name=rxPkts, type=INT64"`
	RxBytes   int64  `parquet:"name=rxBytes, type=INT64"`
}

// ConfigAuditLog represents configuration audit events. Specialized nested types
// are represented as generic values and stringified for storage.
type ConfigAuditLog struct {
	DeferredAt    time.Time `json:"deferredAt,omitzero"`
	EventGroupID  string    `json:"eventGroupID"`
	Origin        string    `json:"origin"`
	Actor         any       `json:"actor"`
	Target        any       `json:"target"`
	Action        string    `json:"action"`
	Old           any       `json:"old,omitzero"`
	New           any       `json:"new,omitzero"`
	ActionDetails string    `json:"actionDetails,omitzero"`
	Error         string    `json:"error,omitzero"`
}

type ParquetConfigLogRow struct {
	TimeRaw       string `parquet:"name=time, type=BYTE_ARRAY, convertedtype=UTF8"`
	RecordedMs    int64  `parquet:"name=recorded_ms, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	DeferredAtMs  int64  `parquet:"name=deferred_at_ms, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	EventGroupID  string `parquet:"name=event_group_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Origin        string `parquet:"name=origin, type=BYTE_ARRAY, convertedtype=UTF8"`
	Actor         string `parquet:"name=actor, type=BYTE_ARRAY, convertedtype=UTF8"`
	Target        string `parquet:"name=target, type=BYTE_ARRAY, convertedtype=UTF8"`
	Action        string `parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8"`
	OldJSON       string `parquet:"name=old, type=BYTE_ARRAY, convertedtype=UTF8"`
	NewJSON       string `parquet:"name=new, type=BYTE_ARRAY, convertedtype=UTF8"`
	ActionDetails string `parquet:"name=action_details, type=BYTE_ARRAY, convertedtype=UTF8"`
	Error         string `parquet:"name=error, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventJSON     string `parquet:"name=event, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// StructureEvent converts EventJSON to structured format preserving nested structure.
func StructureEvent(ev EventJSON) ParquetStructuredNetlogRow {
	convertTraffic := func(traffic []ConnectionCountsJSON) *TrafficStruct {
		if len(traffic) == 0 {
			return nil
		}

		var conns []ConnectionCountsStruct
		for _, cc := range traffic {
			conns = append(conns, ConnectionCountsStruct{
				Proto:     int64(cc.Proto),
				Src:       cc.Src,
				Dst:       cc.Dst,
				TxPackets: int64(cc.TxPackets),
				TxBytes:   int64(cc.TxBytes),
				RxPackets: int64(cc.RxPackets),
				RxBytes:   int64(cc.RxBytes),
			})
		}

		// Use the first connection's proto/src/dst as representative.
		var proto int64
		var src, dst string
		if len(conns) > 0 {
			proto = conns[0].Proto
			src = conns[0].Src
			dst = conns[0].Dst
		}

		return &TrafficStruct{
			Proto: proto,
			Src:   src,
			Dst:   dst,
			Conns: conns,
		}
	}

	return ParquetStructuredNetlogRow{
		Logtail: &LogtailStruct{
			ID:       "", // not present in event form
			ClientID: "", // not present in event form
		},
		NodeID:          ev.NodeID,
		Start:           ev.Start.Format(time.RFC3339),
		End:             ev.End.Format(time.RFC3339),
		SubnetTraffic:   convertTraffic(ev.SubnetTraffic),
		PhysicalTraffic: convertTraffic(ev.PhysicalTraffic),
		ExitTraffic:     convertTraffic(ev.ExitTraffic),
		VirtualTraffic:  convertTraffic(ev.VirtualTraffic),
		Text:            "",
	}
}
