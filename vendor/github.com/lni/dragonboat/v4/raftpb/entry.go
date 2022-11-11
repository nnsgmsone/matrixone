// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: raft.proto

package raftpb

type Entry struct {
	Term        uint64
	Index       uint64
	Type        EntryType
	Key         uint64
	ClientID    uint64
	SeriesID    uint64
	RespondedTo uint64
	Cmd         []byte
}

func (m *Entry) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func EntriesToApply(entries []Entry, applied uint64, strict bool) []Entry {
	if len(entries) == 0 {
		return entries
	}
	lastIndex := entries[len(entries)-1].Index
	firstIndex := entries[0].Index
	if lastIndex <= applied {
		if strict {
			plog.Panicf("got entries [%d-%d] older than current state %d",
				firstIndex, lastIndex, applied)
		}
		return []Entry{}
	}
	if firstIndex > applied+1 {
		plog.Panicf("entry hole found: %d, want: %d", firstIndex, applied+1)
	}
	if applied-firstIndex+1 < uint64(len(entries)) {
		return entries[applied-firstIndex+1:]
	}
	return []Entry{}
}
