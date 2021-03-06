// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"reflect"
	"testing"
)

func TestFindConflict(t *testing.T) {
	previousEnts := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	tests := []struct {
		ents      []Entry
		wconflict int
	}{
		// no conflict, empty ent
		{[]Entry{}, 0},
		{[]Entry{}, 0},
		// no conflict
		{[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]Entry{{Index: 3, Term: 3}}, 0},
		// no conflict, but has new entries
		{[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]Entry{{Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		// conflicts with existing entries
		{[]Entry{{Index: 1, Term: 4}, {Index: 2, Term: 4}}, 1},
		{[]Entry{{Index: 2, Term: 1}, {Index: 3, Term: 4}, {Index: 4, Term: 4}}, 2},
		{[]Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}, {Index: 5, Term: 4}, {Index: 6, Term: 4}}, 3},
	}

	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append(previousEnts...)

		gconflict := raftLog.findConflict(tt.ents)
		if gconflict != tt.wconflict {
			t.Errorf("#%d: conflict = %d, want %d", i, gconflict, tt.wconflict)
		}
	}
}

func TestIsUpToDate(t *testing.T) {
	previousEnts := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	raftLog := newLog(NewMemoryStorage())
	raftLog.append(previousEnts...)
	tests := []struct {
		lastIndex int
		term      int
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		gUpToDate := raftLog.isUpToDate(tt.lastIndex, tt.term)
		if gUpToDate != tt.wUpToDate {
			t.Errorf("#%d: uptodate = %v, want %v", i, gUpToDate, tt.wUpToDate)
		}
	}
}

func TestAppend(t *testing.T) {
	previousEnts := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	tests := []struct {
		ents      []Entry
		windex    int
		wents     []Entry
		wunstable int
	}{
		{
			[]Entry{},
			2,
			[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			3,
		},
		{
			[]Entry{{Index: 3, Term: 2}},
			3,
			[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}},
			3,
		},
		// conflicts with index 1
		{
			[]Entry{{Index: 1, Term: 2}},
			1,
			[]Entry{{Index: 1, Term: 2}},
			1,
		},
		// conflicts with index 2
		{
			[]Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}},
			3,
			[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}},
			2,
		},
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(previousEnts)
		raftLog := newLog(storage)

		index := raftLog.append(tt.ents...)
		if index != tt.windex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, index, tt.windex)
		}
		if g := raftLog.entries(1); !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: logEnts = %+v, want %+v", i, g, tt.wents)
		}
		if g := raftLog.unstable.offset; g != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, g, tt.wunstable)
		}
	}
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
// 	1. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it
// 	2.Append any new entries not already in the log
// If the given (index, term) does not match with the existing log:
// 	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := int(3)
	lastterm := int(3)
	commit := int(1)

	tests := []struct {
		logTerm   int
		index     int
		committed int
		ents      []Entry

		wlasti  int
		wappend bool
		wcommit int
		wpanic  bool
	}{
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, []Entry{{Index: lastindex + 1, Term: 4}},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, []Entry{{Index: lastindex + 2, Term: 4}},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, nil,
			lastindex, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, nil,
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex - 1, nil,
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			lastterm, lastindex, 0, nil,
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			0, 0, lastindex, nil,
			0, true, commit, false, // commit do not decrease
		},
		{
			lastterm, lastindex, lastindex, []Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, []Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			lastterm, lastindex, lastindex + 2, []Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex + 2, []Entry{{Index: lastindex + 1, Term: 4}, {Index: lastindex + 2, Term: 4}},
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the the entry in the middle
		{
			lastterm - 1, lastindex - 1, lastindex, []Entry{{Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []Entry{{Index: lastindex - 1, Term: 4}},
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			lastterm - 3, lastindex - 3, lastindex, []Entry{{Index: lastindex - 2, Term: 4}},
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []Entry{{Index: lastindex - 1, Term: 4}, {Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append(previousEnts...)
		raftLog.committed = commit
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wpanic != true {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			glasti, gappend := raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents...)
			gcommit := raftLog.committed

			if glasti != tt.wlasti {
				t.Errorf("#%d: lastindex = %d, want %d", i, glasti, tt.wlasti)
			}
			if gappend != tt.wappend {
				t.Errorf("#%d: append = %v, want %v", i, gappend, tt.wappend)
			}
			if gcommit != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, gcommit, tt.wcommit)
			}
			if gappend && len(tt.ents) != 0 {
				gents := raftLog.slice(raftLog.lastIndex()-int(len(tt.ents))+1, raftLog.lastIndex()+1)
				if !reflect.DeepEqual(tt.ents, gents) {
					t.Errorf("%d: appended entries = %v, want %v", i, gents, tt.ents)
				}
			}
		}()
	}
}

// TestCompactionSideEffects ensures that all the log related funcationality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	var i int
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	lastIndex := int(1000)
	unstableIndex := int(750)
	lastTerm := lastIndex
	storage := NewMemoryStorage()
	for i = 1; i <= unstableIndex; i++ {
		storage.Append([]Entry{{Term: int(i), Index: int(i)}})
	}
	raftLog := newLog(storage)
	for i = unstableIndex; i < lastIndex; i++ {
		raftLog.append(Entry{Term: int(i + 1), Index: int(i + 1)})
	}

	ok := raftLog.maybeCommit(lastIndex, lastTerm)
	if !ok {
		t.Fatalf("maybeCommit returned false")
	}
	raftLog.appliedTo(raftLog.committed)

	offset := int(500)
	storage.Compact(offset, nil, nil)

	if raftLog.lastIndex() != lastIndex {
		t.Errorf("lastIndex = %d, want %d", raftLog.lastIndex(), lastIndex)
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		if raftLog.term(j) != j {
			t.Errorf("term(%d) = %d, want %d", j, raftLog.term(j), j)
		}
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		if !raftLog.matchTerm(j, j) {
			t.Errorf("matchTerm(%d) = false, want true", j)
		}
	}

	unstableEnts := raftLog.unstableEntries()
	if g := len(unstableEnts); g != 250 {
		t.Errorf("len(unstableEntries) = %d, want = %d", g, 250)
	}
	if unstableEnts[0].Index != 751 {
		t.Errorf("Index = %d, want = %d", unstableEnts[0].Index, 751)
	}

	prev := raftLog.lastIndex()
	raftLog.append(Entry{Index: raftLog.lastIndex() + 1, Term: raftLog.lastIndex() + 1})
	if raftLog.lastIndex() != prev+1 {
		t.Errorf("lastIndex = %d, want = %d", raftLog.lastIndex(), prev+1)
	}

	ents := raftLog.entries(raftLog.lastIndex())
	if len(ents) != 1 {
		t.Errorf("len(entries) = %d, want = %d", len(ents), 1)
	}
}

func TestNextEnts(t *testing.T) {
	snap := Snapshot{
		Metadata: SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied int
		wents   []Entry
	}{
		{0, ents[:2]},
		{3, ents[:2]},
		{4, ents[1:2]},
		{5, nil},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.ApplySnapshot(snap)
		raftLog := newLog(storage)
		raftLog.append(ents...)
		raftLog.maybeCommit(5, 1)
		raftLog.appliedTo(tt.applied)

		nents := raftLog.nextEnts()
		if !reflect.DeepEqual(nents, tt.wents) {
			t.Errorf("#%d: nents = %+v, want %+v", i, nents, tt.wents)
		}
	}
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestUnstableEnts(t *testing.T) {
	previousEnts := []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		unstable int
		wents    []Entry
	}{
		{3, nil},
		{1, previousEnts},
	}

	for i, tt := range tests {
		// append stable entries to storage
		storage := NewMemoryStorage()
		storage.Append(previousEnts[:tt.unstable-1])

		// append unstable entries to raftlog
		raftLog := newLog(storage)
		raftLog.append(previousEnts[tt.unstable-1:]...)

		ents := raftLog.unstableEntries()
		if l := len(ents); l > 0 {
			raftLog.stableTo(ents[l-1].Index, ents[l-i].Term)
		}
		if !reflect.DeepEqual(ents, tt.wents) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, ents, tt.wents)
		}
		w := previousEnts[len(previousEnts)-1].Index + 1
		if g := raftLog.unstable.offset; g != w {
			t.Errorf("#%d: unstable = %d, want %d", i, g, w)
		}
	}
}

func TestCommitTo(t *testing.T) {
	previousEnts := []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}
	commit := int(2)
	tests := []struct {
		commit  int
		wcommit int
		wpanic  bool
	}{
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	}
	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wpanic != true {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			raftLog := newLog(NewMemoryStorage())
			raftLog.append(previousEnts...)
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			if raftLog.committed != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, raftLog.committed, tt.wcommit)
			}
		}()
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei   int
		stablet   int
		wunstable int
	}{
		{1, 1, 2},
		{2, 2, 3},
		{2, 1, 1}, // bad term
		{3, 1, 1}, // bad index
	}
	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}...)
		raftLog.stableTo(tt.stablei, tt.stablet)
		if raftLog.unstable.offset != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.unstable.offset, tt.wunstable)
		}
	}
}

func TestStableToWithSnap(t *testing.T) {
	snapi, snapt := int(5), int(2)
	tests := []struct {
		stablei int
		stablet int
		newEnts []Entry

		wunstable int
	}{
		{snapi + 1, snapt, nil, snapi + 1},
		{snapi, snapt, nil, snapi + 1},
		{snapi - 1, snapt, nil, snapi + 1},

		{snapi + 1, snapt + 1, nil, snapi + 1},
		{snapi, snapt + 1, nil, snapi + 1},
		{snapi - 1, snapt + 1, nil, snapi + 1},

		{snapi + 1, snapt, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 2},
		{snapi, snapt, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},

		{snapi + 1, snapt + 1, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi, snapt + 1, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt + 1, []Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
	}
	for i, tt := range tests {
		s := NewMemoryStorage()
		s.ApplySnapshot(Snapshot{Metadata: SnapshotMetadata{Index: snapi, Term: snapt}})
		raftLog := newLog(s)
		raftLog.append(tt.newEnts...)
		raftLog.stableTo(tt.stablei, tt.stablet)
		if raftLog.unstable.offset != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.unstable.offset, tt.wunstable)
		}
	}
}

//TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	tests := []struct {
		lastIndex int
		compact   []int
		wleft     []int
		wallow    bool
	}{
		// out of upper bound
		{1000, []int{1001}, []int{-1}, false},
		{1000, []int{300, 500, 800, 900}, []int{700, 500, 200, 100}, true},
		// out of lower bound
		{1000, []int{300, 299}, []int{700, -1}, false},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow == true {
						t.Errorf("%d: allow = %v, want %v: %v", i, false, true, r)
					}
				}
			}()

			storage := NewMemoryStorage()
			for i := int(1); i <= tt.lastIndex; i++ {
				storage.Append([]Entry{{Index: i}})
			}
			raftLog := newLog(storage)
			raftLog.maybeCommit(tt.lastIndex, 0)
			raftLog.appliedTo(raftLog.committed)

			for j := 0; j < len(tt.compact); j++ {
				err := storage.Compact(tt.compact[j], nil, nil)
				if err != nil {
					if tt.wallow {
						t.Errorf("#%d.%d allow = %t, want %t", i, j, false, tt.wallow)
					}
					continue
				}
				if len(raftLog.allEntries()) != tt.wleft[j] {
					t.Errorf("#%d.%d len = %d, want %d", i, j, len(raftLog.allEntries()), tt.wleft[j])
				}
			}
		}()
	}
}

func TestLogRestore(t *testing.T) {
	index := int(1000)
	term := int(1000)
	snap := SnapshotMetadata{Index: index, Term: term}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(Snapshot{Metadata: snap})
	raftLog := newLog(storage)

	if len(raftLog.allEntries()) != 0 {
		t.Errorf("len = %d, want 0", len(raftLog.allEntries()))
	}
	if raftLog.firstIndex() != index+1 {
		t.Errorf("firstIndex = %d, want %d", raftLog.firstIndex(), index+1)
	}
	if raftLog.committed != index {
		t.Errorf("committed = %d, want %d", raftLog.committed, index)
	}
	if raftLog.unstable.offset != index+1 {
		t.Errorf("unstable = %v, want %d", raftLog.unstable, index+1)
	}
	if raftLog.term(index) != term {
		t.Errorf("term = %d, want %d", raftLog.term(index), term)
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := int(100)
	num := int(100)
	storage := NewMemoryStorage()
	storage.ApplySnapshot(Snapshot{Metadata: SnapshotMetadata{Index: offset}})
	l := newLog(storage)
	for i := int(1); i <= num; i++ {
		l.append(Entry{Index: i + offset})
	}

	first := offset + 1
	tests := []struct {
		lo, hi int
		wpainc bool
	}{
		{
			first - 2, first + 1,
			true,
		},
		{
			first - 1, first + 1,
			true,
		},
		{
			first, first,
			false,
		},
		{
			first + num/2, first + num/2,
			false,
		},
		{
			first + num - 1, first + num - 1,
			false,
		},
		{
			first + num, first + num,
			false,
		},
		{
			first + num, first + num + 1,
			true,
		},
		{
			first + num + 1, first + num + 1,
			true,
		},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpainc {
						t.Errorf("%d: panic = %v, want %v: %v", i, true, false, r)
					}
				}
			}()
			l.mustCheckOutOfBounds(tt.lo, tt.hi)
			if tt.wpainc {
				t.Errorf("%d: panic = %v, want %v", i, false, true)
			}
		}()
	}
}

func TestTerm(t *testing.T) {
	var i int
	offset := int(100)
	num := int(100)

	storage := NewMemoryStorage()
	storage.ApplySnapshot(Snapshot{Metadata: SnapshotMetadata{Index: offset, Term: 1}})
	l := newLog(storage)
	for i = 1; i < num; i++ {
		l.append(Entry{Index: offset + i, Term: i})
	}

	tests := []struct {
		index int
		w     int
	}{
		{offset - 1, 0},
		{offset, 1},
		{offset + num/2, num / 2},
		{offset + num - 1, num - 1},
		{offset + num, 0},
	}

	for j, tt := range tests {
		term := l.term(tt.index)
		if !reflect.DeepEqual(term, tt.w) {
			t.Errorf("#%d: at = %d, want %d", j, term, tt.w)
		}
	}
}

func TestTermWithUnstableSnapshot(t *testing.T) {
	storagesnapi := int(100)
	unstablesnapi := storagesnapi + 5

	storage := NewMemoryStorage()
	storage.ApplySnapshot(Snapshot{Metadata: SnapshotMetadata{Index: storagesnapi, Term: 1}})
	l := newLog(storage)
	l.restore(Snapshot{Metadata: SnapshotMetadata{Index: unstablesnapi, Term: 1}})

	tests := []struct {
		index int
		w     int
	}{
		// cannot get term from storage
		{storagesnapi, 0},
		// cannot get term from the gap between storage ents and unstable snapshot
		{storagesnapi + 1, 0},
		{unstablesnapi - 1, 0},
		// get term from unstable snapshot index
		{unstablesnapi, 1},
	}

	for i, tt := range tests {
		term := l.term(tt.index)
		if !reflect.DeepEqual(term, tt.w) {
			t.Errorf("#%d: at = %d, want %d", i, term, tt.w)
		}
	}
}

func TestSlice(t *testing.T) {
	var i int
	offset := int(100)
	num := int(100)

	storage := NewMemoryStorage()
	storage.ApplySnapshot(Snapshot{Metadata: SnapshotMetadata{Index: offset}})
	l := newLog(storage)
	for i = 1; i < num; i++ {
		l.append(Entry{Index: offset + i, Term: offset + i})
	}

	tests := []struct {
		from   int
		to     int
		w      []Entry
		wpanic bool
	}{
		{offset - 1, offset + 1, nil, true},
		{offset, offset + 1, nil, true},
		{offset + num/2, offset + num/2 + 1, []Entry{{Index: offset + num/2, Term: offset + num/2}}, false},
		{offset + num - 1, offset + num, []Entry{{Index: offset + num - 1, Term: offset + num - 1}}, false},
		{offset + num, offset + num + 1, nil, true},
	}

	for j, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", j, true, false, r)
					}
				}
			}()
			g := l.slice(tt.from, tt.to)
			if !reflect.DeepEqual(g, tt.w) {
				t.Errorf("#%d: from %d to %d = %v, want %v", j, tt.from, tt.to, g, tt.w)
			}
		}()
	}
}
