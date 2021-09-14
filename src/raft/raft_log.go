package raft

// 2D
type RaftLog struct {
	Log               []*Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (l *RaftLog) Len() int {
	return l.LastIncludedIndex + len(l.Log)
}

func (l *RaftLog) Append(e ...*Entry) {
	l.Log = append(l.Log, e...)
}

func (l *RaftLog) Get(idx int) *Entry {
	DPrintf("Get opt:idx %d, lastIndex: %d", idx, l.LastIncludedIndex)
	return l.Log[idx-l.LastIncludedIndex-1]
}

// use copy to GC the underlying big array
func (l *RaftLog) SliceToTail(idx int) []*Entry {
	if idx > l.Len() {
		return []*Entry{}
	}
	copied := make([]*Entry, l.Len()-idx+1)
	copy(copied, l.Log[idx-l.LastIncludedIndex-1:])
	return copied
}

func (l *RaftLog) Slice(left, right int) []*Entry {
	if left > l.Len() || right <= l.LastIncludedIndex+1 {
		return []*Entry{}
	}
	left = max(left, l.LastIncludedIndex+1)
	copied := make([]*Entry, right-left)
	copy(copied, l.Log[left-1-l.LastIncludedIndex:right-1-l.LastIncludedIndex])
	return copied
}

func (l *RaftLog) GetTerm(idx int) int {
	if idx > l.LastIncludedIndex {
		return l.Get(idx).Term
	}
	return l.LastIncludedTerm
}
