package raft

import (
	pb "github.com/Alorun/stonekv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries that not truncated

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	l := &RaftLog{storage: storage}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// Load entries from storage
	if lastIndex >= firstIndex {
		entries, err := storage.Entries(firstIndex,lastIndex + 1)
		if err != nil {
			panic(err)
		}
		l.entries = entries
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	l.stabled = lastIndex
	l.committed = min(hardState.Commit, lastIndex)
	l.applied = firstIndex - 1

	return nil
}

// offset return the index of the entries[0] in the global log.
// If entries is empty, it return stable + 1
func (l *RaftLog) offset() uint64 {
	if len(l.entries) == 0 {
		return l.stabled + 1
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Having pendingSnapshot more up-to-data than entries
	if !IsEmptySnap(l.pendingSnapshot) {
		snapIndex := l.pendingSnapshot.Metadata.Index
		if len(l.entries) == 0 || snapIndex > l.entries[len(l.entries) - 1].Index {
			return snapIndex
		}
	}
	if len(l.entries) == 0 {
		idx, err := l.storage.LastIndex()
		if err != nil {
			panic(err)
		}
		return idx
	}
	return l.entries[len(l.entries) - 1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Checking pendingSnapshot
	if !IsEmptySnap(l.pendingSnapshot) {
		meta := l.pendingSnapshot.Metadata
		if i == meta.Index {
			return meta.Term, nil
		}
		if i < meta.Index {
			return 0, ErrCompacted
		}
	}
	
	// Finding from entrise
	off := l.offset()
	if len(l.entries) > 0 && i >= off {
		idx := i - off
		if idx < uint64(len(l.entries)) {
			return l.entries[idx].Term, nil
		}
	}

	t, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return t, nil
}


// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	off := l.offset()
	if l.stabled + 1 < off {
		return l.entries
	}
	if l.stabled >= l.entries[len(l.entries) - 1].Index {
		return nil
	}
	return l.entries[l.stabled + 1 - off:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	off := l.offset()
	if l.committed + 1 <= l.applied || len(l.entries) == 0 {
		return nil
	}
	lo := max(l.applied + 1, off)
	hi := l.committed + 1
	if lo >= hi || lo < off || hi - off > uint64(len(l.entries)) {
		return nil
	}
	return l.entries[lo - off : hi - off]
}

// appends new entries to the memory log and handles conflict truncation
func (l *RaftLog) append(entries ...pb.Entry) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	off := l.offset()

	switch {
	case len(l.entries) == 0 || first > l.entries[len(l.entries) - 1].Index + 1:
		// Normally, add directly
		l.entries = append(l.entries, entries...)
	case first < off:
		// New entries will start from before the snapshot and replace all existing entries
		l.entries = make([]pb.Entry, len(entries))
		copy(l.entries, entries)
		l.stabled = first - 1	
	default:
		// Conflict exist, truncate the content after the conflict point
		keep := l.entries[:first - off]
		l.entries = append([]pb.Entry{}, keep...)
		l.entries = append(l.entries, entries...)
		if l.stabled >= first {
			l.stabled = first - 1
		}
	}
}

// maybeCommit attempts to advance commitIndex to toCommit
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	t, err := l.Term(toCommit)
	if err == nil && t == term && toCommit > l.committed {
		l.committed = toCommit
		return true
	}
	return false
}

// appliedTo advances the applied pointer
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return 
	}
	if i > l.committed || i < l.applied {
		panic("appliedTo out of range")
	}
	l.applied = i
}

// stableTo marks entries up to i as persisted, advances them to stabled
func (l *RaftLog) stableTo(i, t uint64) {
	gt, err := l.Term(i)
	if err != nil || gt != t{
		return
	}
	if i > l.stabled {
		l.stabled = i
	}
}

// slice returns log entries in the range [lo, hi)
func (l* RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		return nil, nil
	}
	off := l.offset()
	if len(l.entries) == 0 || lo < off {
		// Need to read from storage
		storedEnts, err := l.storage.Entries(lo, hi)
		if err != nil {
			return nil, err
		}
		return storedEnts, nil
	}
	return l.entries[lo - off : hi - off], nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	off := l.offset()
	if firstIndex > off && len(l.entries) > 0 {
		l.entries = l.entries[firstIndex - off:]
	}
}
