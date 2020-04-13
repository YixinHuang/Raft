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
	"fmt"
)

type Progress struct {
	Match, Next int
	Wait        int
}

func (pr *Progress) update(n int) {
	pr.waitReset()
	if pr.Match < n {
		pr.Match = n
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
}

func (pr *Progress) optimisticUpdate(n int) { pr.Next = n + 1 }

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
func (pr *Progress) maybeDecrTo(rejected, last int) bool {
	pr.waitReset()
	if pr.Match != 0 {
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// directly decrease next to match + 1
		pr.Next = pr.Match + 1
		return true
	}

	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next-1 != rejected {
		return false
	}

	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	return true
}

func (pr *Progress) waitDecr(i int) {
	pr.Wait -= i
	if pr.Wait < 0 {
		pr.Wait = 0
	}
}
func (pr *Progress) waitSet(w int)    { pr.Wait = w }
func (pr *Progress) waitReset()       { pr.Wait = 0 }
func (pr *Progress) shouldWait() bool { return pr.Match == 0 && pr.Wait > 0 }

func (pr *Progress) String() string {
	return fmt.Sprintf(" Progress:next = [%d], match = [%d], wait = [%v]", pr.Next, pr.Match, pr.Wait)
}
