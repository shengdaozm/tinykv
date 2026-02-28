package eraftpb

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
)

func FormatMessage(m *Message) string {
	if m == nil {
		return "<nil>"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Message{\n"))
	b.WriteString(fmt.Sprintf("  Type:    %s\n", m.MsgType.String()))
	b.WriteString(fmt.Sprintf("  From:    %d\n", m.From))
	b.WriteString(fmt.Sprintf("  To:      %d\n", m.To))
	b.WriteString(fmt.Sprintf("  Term:    %d\n", m.Term))
	b.WriteString(fmt.Sprintf("  Index:   %d\n", m.Index))
	b.WriteString(fmt.Sprintf("  LogTerm: %d\n", m.LogTerm))
	b.WriteString(fmt.Sprintf("  Commit:  %d\n", m.Commit))
	b.WriteString(fmt.Sprintf("  Reject:  %v\n", m.Reject))
	if len(m.Entries) > 0 {
		b.WriteString("  Entries:\n")
		for i, e := range m.Entries {
			data := e.Data
			if len(data) > 20 {
				data = data[:20]
			}
			b.WriteString(fmt.Sprintf("    [%d] Index:%d Term:%d Type:%s Data:%q\n",
				i, e.Index, e.Term, e.EntryType.String(), data))
		}
	}
	if m.Snapshot != nil {
		meta := m.Snapshot.GetMetadata()
		if meta != nil {
			b.WriteString(fmt.Sprintf("  Snapshot: Index:%d Term:%d\n",
				meta.GetIndex(), meta.GetTerm()))
		}
	}
	b.WriteString("}")
	return b.String()
}

func FormatEntry(e *Entry) string {
	if e == nil {
		return "<nil>"
	}
	data := e.Data
	if len(data) > 20 {
		data = data[:20]
	}
	return fmt.Sprintf("Entry{Index:%d Term:%d Type:%s Data:%q}", e.Index, e.Term, e.EntryType.String(), data)
}

func FormatSnapshot(s *Snapshot) string {
	if s == nil {
		return "<nil>"
	}
	return proto.MarshalTextString(s)
}

func FormatMessages(msgs []*Message) string {
	if len(msgs) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[\n")
	for _, m := range msgs {
		b.WriteString("  ")
		b.WriteString(FormatMessage(m))
		b.WriteString(",\n")
	}
	b.WriteString("]")
	return b.String()
}

func FormatMessageSlice(msgs []Message) string {
	if len(msgs) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[\n")
	for i := range msgs {
		b.WriteString("  ")
		b.WriteString(FormatMessage(&msgs[i]))
		b.WriteString(",\n")
	}
	b.WriteString("]")
	return b.String()
}

type MessageSlice []Message

func (s MessageSlice) String() string { return FormatMessageSlice(s) }
func (s MessageSlice) Len() int       { return len(s) }
func (s MessageSlice) Less(i, j int) bool {
	return s[i].MsgType < s[j].MsgType
}
func (s MessageSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
