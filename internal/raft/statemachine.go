package raft

import "fmt"

// Role describes the current state of a Raft node.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

// Entry is a single Raft log entry.
type Entry struct {
	Term      uint64
	Operation string
	Key       string
	Value     string
}

// VoteRequest models a RequestVote RPC.
type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex int
	LastLogTerm  uint64
}

// VoteResponse models a RequestVote RPC response.
type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest models an AppendEntries RPC.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesResponse models an AppendEntries RPC response.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// StateMachine contains the pure Raft transition logic.
type StateMachine struct {
	id          string
	peers       []string
	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string
	log         []Entry
	commitIndex int
	lastApplied int
	votesSeen   map[string]struct{}
}

// NewStateMachine creates a new Raft node state machine.
func NewStateMachine(id string, peers []string) *StateMachine {
	return &StateMachine{
		id:          id,
		peers:       append([]string(nil), peers...),
		role:        RoleFollower,
		commitIndex: -1,
		lastApplied: -1,
		votesSeen:   make(map[string]struct{}),
	}
}

// ID returns the local node identifier.
func (s *StateMachine) ID() string {
	return s.id
}

// Role returns the current Raft role.
func (s *StateMachine) Role() Role {
	return s.role
}

// CurrentTerm returns the current term.
func (s *StateMachine) CurrentTerm() uint64 {
	return s.currentTerm
}

// VotedFor returns the candidate this node voted for in the current term.
func (s *StateMachine) VotedFor() string {
	return s.votedFor
}

// LeaderID returns the known leader.
func (s *StateMachine) LeaderID() string {
	return s.leaderID
}

// CommitIndex returns the highest known committed log index.
func (s *StateMachine) CommitIndex() int {
	return s.commitIndex
}

// LastApplied returns the highest log index already applied to the state machine.
func (s *StateMachine) LastApplied() int {
	return s.lastApplied
}

// Log returns a copy of the current log.
func (s *StateMachine) Log() []Entry {
	return append([]Entry(nil), s.log...)
}

// EntryAt returns the log entry at index if it exists.
func (s *StateMachine) EntryAt(index int) (Entry, bool) {
	if index < 0 || index >= len(s.log) {
		return Entry{}, false
	}
	return s.log[index], true
}

// ClusterSize returns the number of nodes in the cluster including this one.
func (s *StateMachine) ClusterSize() int {
	return len(s.peers) + 1
}

// SetPeers replaces the peer set used for quorum calculations.
func (s *StateMachine) SetPeers(peers []string) {
	s.peers = append([]string(nil), peers...)
}

// StartElection transitions the node into candidate mode and requests votes.
func (s *StateMachine) StartElection() VoteRequest {
	s.currentTerm++
	s.role = RoleCandidate
	s.votedFor = s.id
	s.leaderID = ""
	s.votesSeen = map[string]struct{}{
		s.id: {},
	}
	if len(s.votesSeen) >= s.majority() {
		s.role = RoleLeader
		s.leaderID = s.id
	}

	lastIndex, lastTerm := s.lastLogInfo()
	return VoteRequest{
		Term:         s.currentTerm,
		CandidateID:  s.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
}

// HandleVoteRequest processes a RequestVote message.
func (s *StateMachine) HandleVoteRequest(req VoteRequest) VoteResponse {
	if req.Term < s.currentTerm {
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	if req.Term > s.currentTerm {
		s.becomeFollower(req.Term, "")
	}

	if !s.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	if s.votedFor != "" && s.votedFor != req.CandidateID {
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	s.role = RoleFollower
	s.votedFor = req.CandidateID
	s.leaderID = ""
	return VoteResponse{Term: s.currentTerm, VoteGranted: true}
}

// HandleVoteResponse processes a RequestVote response and may promote the node.
func (s *StateMachine) HandleVoteResponse(from string, resp VoteResponse) {
	if resp.Term > s.currentTerm {
		s.becomeFollower(resp.Term, "")
		return
	}

	if s.role != RoleCandidate || resp.Term != s.currentTerm || !resp.VoteGranted {
		return
	}

	s.votesSeen[from] = struct{}{}
	if len(s.votesSeen) >= s.majority() {
		s.role = RoleLeader
		s.leaderID = s.id
	}
}

// HandleAppendEntries processes leader heartbeats and log replication.
func (s *StateMachine) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	if req.Term < s.currentTerm {
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}

	if req.Term > s.currentTerm || s.role != RoleFollower {
		s.becomeFollower(req.Term, req.LeaderID)
	} else {
		s.leaderID = req.LeaderID
	}

	if !s.matchesPreviousLog(req.PrevLogIndex, req.PrevLogTerm) {
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}

	insertAt := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		target := insertAt + i
		if target >= len(s.log) {
			s.log = append(s.log, req.Entries[i:]...)
			break
		}
		if s.log[target].Term != entry.Term {
			s.log = append(s.log[:target], req.Entries[i:]...)
			break
		}
	}

	if req.LeaderCommit > s.commitIndex {
		lastIndex := len(s.log) - 1
		if req.LeaderCommit < lastIndex {
			s.commitIndex = req.LeaderCommit
		} else {
			s.commitIndex = lastIndex
		}
	}

	return AppendEntriesResponse{Term: s.currentTerm, Success: true}
}

// AppendCommand appends a new log entry on the leader.
func (s *StateMachine) AppendCommand(operation, key, value string) (Entry, int, error) {
	if s.role != RoleLeader {
		return Entry{}, -1, fmt.Errorf("append command requires leader role")
	}

	entry := Entry{
		Term:      s.currentTerm,
		Operation: operation,
		Key:       key,
		Value:     value,
	}
	s.log = append(s.log, entry)
	return entry, len(s.log) - 1, nil
}

// ObserveTerm steps the node down if it learns of a newer term.
func (s *StateMachine) ObserveTerm(term uint64, leaderID string) {
	if term > s.currentTerm {
		s.becomeFollower(term, leaderID)
	}
}

// AdvanceCommitIndex marks entries through index as committed.
func (s *StateMachine) AdvanceCommitIndex(index int) {
	if index <= s.commitIndex {
		return
	}
	lastIndex := len(s.log) - 1
	if index > lastIndex {
		index = lastIndex
	}
	s.commitIndex = index
}

// TakeCommittedEntries returns newly committed entries that have not been applied yet.
func (s *StateMachine) TakeCommittedEntries() []Entry {
	if s.commitIndex <= s.lastApplied {
		return nil
	}

	start := s.lastApplied + 1
	end := s.commitIndex + 1
	entries := append([]Entry(nil), s.log[start:end]...)
	s.lastApplied = s.commitIndex
	return entries
}

func (s *StateMachine) becomeFollower(term uint64, leaderID string) {
	s.role = RoleFollower
	s.currentTerm = term
	s.votedFor = ""
	s.leaderID = leaderID
	s.votesSeen = make(map[string]struct{})
}

func (s *StateMachine) majority() int {
	return s.ClusterSize()/2 + 1
}

func (s *StateMachine) lastLogInfo() (int, uint64) {
	if len(s.log) == 0 {
		return -1, 0
	}
	return len(s.log) - 1, s.log[len(s.log)-1].Term
}

func (s *StateMachine) isLogUpToDate(candidateIndex int, candidateTerm uint64) bool {
	lastIndex, lastTerm := s.lastLogInfo()
	if candidateTerm != lastTerm {
		return candidateTerm > lastTerm
	}
	return candidateIndex >= lastIndex
}

func (s *StateMachine) matchesPreviousLog(prevIndex int, prevTerm uint64) bool {
	if prevIndex == -1 {
		return true
	}
	if prevIndex < 0 || prevIndex >= len(s.log) {
		return false
	}
	return s.log[prevIndex].Term == prevTerm
}
