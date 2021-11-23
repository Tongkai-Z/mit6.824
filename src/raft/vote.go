package raft

import (
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
// Receiver implementation: 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// (2A, 2B).
	// 2A
	// FIXME(done) requestVote also set the heartbeat preventing other follower time-out
	// FIXME (done)requestVote only reset heartbeat when it grant the vote
	rf.mu.Lock() // protect rf's field
	// last entry's term
	// TODO (done)if leader or candidate finds out that it's term is out of date
	// it will immediately update its term and turn to follower
	// FIXME (done)update the receiver's term if it's smaller
	// FIXME (done)should we update while we receive voterequest?
	// this may cause the term to increase even no one get elected which is acceptable
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 2
		//clear voted for
		rf.votedFor = -1
		rf.persist()
	}
	lastTerm := 0
	if rf.log.Len() > 0 {
		lastTerm = rf.log.GetTerm(rf.log.Len())
	}
	if rf.currentTerm > args.Term || lastTerm > args.LastLogTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("server %d(%d)reject voting server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		// not candidate
	} else if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId) {
		// candidate is at least up-to-date as receiver
		if lastTerm < args.LastLogTerm || rf.log.Len() <= args.LastLogIndex {
			atomic.StoreInt32(&rf.heartbeat, 1)
			reply.VoteGranted = true
			rf.votedFor = int32(args.CandidateId)
			rf.persist()
			DPrintf("server %d(%d) vote for server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			DPrintf("server %d(%d) rejects server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		}
		reply.Term = args.Term
	} else { // already voted this term
		reply.VoteGranted = false
		reply.Term = args.Term
		DPrintf("server %d(%d) rejects server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}

	rf.mu.Unlock()
}

func (rf *Raft) startElection(termForElection int32) {
	rf.mu.Lock()
	rf.votedFor = int32(rf.me)
	rf.state = 3 // turn to candidate
	rf.currentTerm = termForElection
	rf.persist()
	lastLogIndex := rf.log.Len()
	lastLogTerm := 0
	if rf.log.Len() > 0 {
		lastLogTerm = rf.log.GetTerm(lastLogIndex)
	}
	DPrintf("server %d starts an election for term %d\n, peers: %d, last log term: %d,  current log: %+v", rf.me, rf.currentTerm, len(rf.peers), lastLogTerm, rf.log)
	rf.mu.Unlock()

	respChan := make(chan int)
	voteChan := make(chan int)
	timerChan := make(chan int)
	go func() {
		time.Sleep(200 * time.Millisecond)
		timerChan <- 1
	}()
	count := int32(len(rf.peers) - 1)
	majority := int32(len(rf.peers) / 2)
	DPrintf("server %d needs %d votes", rf.me, majority)
	for index, _ := range rf.peers {
		if index != rf.me {
			go func(idx int) {
				req := &RequestVoteArgs{}
				req.CandidateId = rf.me
				req.Term = termForElection
				req.LastLogIndex = lastLogIndex
				req.LastLogTerm = lastLogTerm
				resp := &RequestVoteReply{}
				ok := rf.sendRequestVote(idx, req, resp)
				// network failure cause the response delayed by 2sec
				// so we shouldn't wait for it to join if we already got majority votes
				// if majority = 0 we can triger the following logic
				// wait for majority votes
				if ok {
					if resp.VoteGranted {
						atomic.AddInt32(&majority, -1)
						if atomic.LoadInt32(&majority) == 0 {
							voteChan <- 1
						}
					} else {
						rf.mu.Lock()
						// when a server find its current term is smaller
						// it would turn into follower
						if rf.currentTerm < resp.Term {
							rf.state = 2
							rf.currentTerm = resp.Term
							rf.persist()
						}
						rf.mu.Unlock()
					}
				} else {
					DPrintf("server %d not respond vote for  leader %d", idx, rf.me)
				}
				atomic.AddInt32(&count, -1)
				if atomic.LoadInt32(&count) == 0 { // guarantee that only one goroutine get 0
					respChan <- 1
				}
			}(index)

		}
	}
	// interval := rand.Intn(30) + 30
	// time.Sleep(time.Duration(interval) * time.Millisecond)
	// since Call() must return, we don't need set the timeout but use waitgroup
	// timeout check majority vote
	// TODOuse select to observe two info: all response / majority granted vote
	select {
	case <-voteChan:
		rf.initializeLeaderState()
	case <-respChan:
		rf.mu.Lock()
		rf.state = 2 // turn to follower
		DPrintf("server %d lose election for term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	case <-timerChan:
		rf.mu.Lock()
		rf.state = 2 // turn to follower
		DPrintf("server %d stop election for term %d, due to time out\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) initializeLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == 3 {
		rf.state = 1 // turn to leader
		// initialize the matchIndex and nextIndex
		rf.nextIndex = make([]int, len(rf.peers))
		for idx, _ := range rf.nextIndex {
			rf.nextIndex[idx] = rf.log.Len() + 1 // initialized to leader last log index + 1
		}
		rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0
		DPrintf("server %d becomes leader in term %d\n", rf.me, rf.currentTerm)
	} else { // already turn to follower due to other term exchange rpc
		DPrintf("server %d lose election for term %d\n", rf.me, rf.currentTerm)
	}
}
