package raft

import "sync/atomic"

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry // empty for heartbeat
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term                        int32
	Success                     bool
	ConflictTerm                int
	FirstConflictTermEntryIndex int
}

// problem for commit: new entries keep comming in
// since log replication would retry indefinitely, we don't need the respChan and commitChan
func (rf *Raft) processLogReplication() {
	// DPrintf("server %d needs %d yes to commit the command", rf.me, majority)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(i int) {
				for !rf.killed() { // only process when rpc call is correctly responsed
					args := &AppendEntriesArgs{}
					rf.buildAppendEntriesArgs(args, i)
					reply := &AppendEntriesReply{}
					_, isLeader := rf.GetState()
					if !isLeader {
						return
					}
					rf.sendAppendEntries(i, args, reply)
					if reply.Success {
						// DPrintf("server %d accepted server %d's append", i, args.LeaderId)
						rf.mu.Lock()
						// nextIndex might be updated already
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.commitIfPossible()
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Lock()
						// when a server find its current term is smaller
						// it would turn into follower
						if rf.currentTerm < reply.Term {
							DPrintf("server %d append rejected by server %d due to lower term", rf.me, i)
							rf.state = 2
							isLeader = false
							rf.currentTerm = reply.Term
							rf.persist()
							rf.mu.Unlock()
							return
						} else {
							// synchronize the log with follower i, can optimize it with term backwards
							// decrease the nextIndex, which is initialized to len(leader_log) + 1
							// append the prev entry at the head and resend the appendRPC
							// nextIndex shouldn't be smaller than matchIndex
							rf.nextIndex[i] = reply.FirstConflictTermEntryIndex
							if rf.nextIndex[i] <= rf.matchIndex[i] {
								rf.nextIndex[i] = rf.matchIndex[i] + 1 // >= 1
							}
							DPrintf("leader %d, follower %d nextIndex %d", rf.me, i, rf.nextIndex[i])
							rf.mu.Unlock()
						}
					}
				}

				//DPrintf("server %d connected with leader %d? %v", i, rf.me, ok)
			}(idx)
		}
	}
}

func (rf *Raft) commitIfPossible() {
	if rf.state == 1 && !rf.killed() {
		// rf.commitIndex = int32(len(rf.log))
		// send the command from old commitIndex to current
		// FIXMEwhat if the new entries came in-between, is it safe to commit the last entry?
		majority := len(rf.peers) / 2
		old := rf.commitIndex
		start := int(rf.commitIndex + 1)
		for idx := start; idx <= len(rf.log); idx++ {
			if rf.log[idx-1].Term == int(rf.currentTerm) { // figure 8
				n := majority
				for i, matchedIdx := range rf.matchIndex {
					if i != rf.me && matchedIdx >= idx {
						n--
					}
				}
				if n <= 0 {
					rf.commitIndex = int32(idx)
				}
			}

		}
		if rf.commitIndex > old {
			go rf.checkApply(rf.commitIndex)
		}

		DPrintf("current log of leader %d: %+v, commit index: %d", rf.me, rf.log, rf.commitIndex)

	}
}

func (rf *Raft) checkApply(target int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	target = minInt(target, int32(len(rf.log)))
	for i := rf.lastApplied + 1; i <= target; i++ {
		var applyMsg ApplyMsg
		applyMsg.Command = rf.log[i-1].Command
		applyMsg.CommandIndex = int(i)
		applyMsg.CommandValid = true
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = target
	DPrintf("entry %d applied for node %d", rf.lastApplied, rf.me)
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO wrap nextIndex till last entry in leader as entries
	// if nextIndex > index of lastEntry then heartbeat
	// entries are the next[i] to last
	args.Entries = []*Entry{}
	// check currentTerm and last entry term
	// only commit the entry at the currentTerm
	// but we can send commit one even if it is stale
	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	DPrintf("check stale entry term, currentTerm: %d, lastEntry Term: %d", rf.currentTerm, lastTerm)
	for j := rf.nextIndex[peer] - 1; j < len(rf.log); j++ {
		if j < int(rf.commitIndex) || lastTerm == int(rf.currentTerm) {
			//rf.log[i] should be copied to avoid shared memory
			args.Entries = append(args.Entries, rf.log[j])
		}
	}
	//DPrintf("args entries: %+v", args.Entries)
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.PrevLogTerm = 0
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
}

// This function appendEntries to followers
// note that this message also establish the leadership, so it will clear the voteFor
// if the current server is a candidate, it will compare the term
// if currentTerm <= receivedTerm it would turn to follower at once
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// update term first
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d received leader %d append request: %+v", rf.me, args.LeaderId, args)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// sender's term is at least as big as the receiver
	// turn to follower and clear the votefor
	rf.state = 2
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.persist()
	atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)
	// compare the log and commitIndex
	// only when log is consistent can we implement the commit logic
	prevTerm := 0
	if len(rf.log) >= args.PrevLogIndex && len(rf.log) > 0 && args.PrevLogIndex > 0 {
		prevTerm = rf.log[args.PrevLogIndex-1].Term
		//DPrintf("server %d received appendEntries, term: %d, %d", rf.me, prevTerm, args.PrevLogTerm)
	}

	if prevTerm == args.PrevLogTerm {
		// append entries from prevLogIndex
		if len(args.Entries) > 0 {
			DPrintf("server %d entries to append from server %d, entries: %v, prevTerm: %d, prevIndex: %d, leaderterm: %d, followerTerm: %d", rf.me, args.LeaderId, args.Entries, args.PrevLogTerm, args.PrevLogIndex, args.Term, rf.currentTerm)
			// 2C figure8 unreliable: can't just truncate, but check first
			// if the entry in the log is not conflict, we shouldn't use the leader's
			// find the first conflict entry in the follower's log
			currentIdx := args.PrevLogIndex + 1
			entryIdx := 0
			for currentIdx <= len(rf.log) && entryIdx < len(args.Entries) {
				if rf.log[currentIdx-1].Term != args.Entries[entryIdx].Term {
					break
				} else {
					entryIdx++
					currentIdx++
				}
			}
			if entryIdx < len(args.Entries) {
				rf.log = append(rf.log[0:currentIdx-1], args.Entries[entryIdx:]...)
				rf.persist()
				DPrintf("server %d append successfully, log: %+v", rf.me, rf.log)
			}
		} else {
			DPrintf("server %d received heartbeat", rf.me)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			go rf.checkApply(args.LeaderCommit)
		}
		reply.Success = true

	} else {
		reply.Success = false
		DPrintf("server %d rejected leader %d, log: %+v, prevIndex: %d, prevTerm: %d, args.prevTerm: %d", rf.me, args.LeaderId, rf.log, args.PrevLogIndex, prevTerm, args.PrevLogTerm)
		//reply the conflict term and the first entry for that term
		reply.ConflictTerm = prevTerm
		if len(rf.log) < args.PrevLogIndex {
			reply.FirstConflictTermEntryIndex = len(rf.log)
		} else if prevTerm > 0 {
			// find the first entry in that term
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if i == 0 || rf.log[i-1].Term < prevTerm {
					reply.FirstConflictTermEntryIndex = i + 1
				}
			}
		} else {
			reply.FirstConflictTermEntryIndex = 1
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// check if rf is still the leader
	// avoid the bug that buildentry with a new updated term number
	DPrintf("server %d send to server %d args: %+v", req.LeaderId, server, req)
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}
