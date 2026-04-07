package raft

import "testing"

func TestStartElectionPromotesNodeToCandidate(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})

	req := node.StartElection()

	if node.Role() != RoleCandidate {
		t.Fatalf("expected candidate role, got %s", node.Role())
	}
	if node.CurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", node.CurrentTerm())
	}
	if node.VotedFor() != "node-1" {
		t.Fatalf("expected self vote, got %q", node.VotedFor())
	}
	if req.Term != 1 || req.CandidateID != "node-1" || req.LastLogIndex != -1 || req.LastLogTerm != 0 {
		t.Fatalf("unexpected vote request: %+v", req)
	}
}

func TestHandleVoteRequestGrantsVoteToUpToDateCandidate(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})

	resp := node.HandleVoteRequest(VoteRequest{
		Term:         3,
		CandidateID:  "node-1",
		LastLogIndex: -1,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Fatalf("expected vote to be granted")
	}
	if node.CurrentTerm() != 3 {
		t.Fatalf("expected term 3, got %d", node.CurrentTerm())
	}
	if node.VotedFor() != "node-1" {
		t.Fatalf("expected vote recorded for node-1, got %q", node.VotedFor())
	}
}

func TestHandleVoteRequestRejectsStaleTerm(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})
	node.StartElection()

	resp := node.HandleVoteRequest(VoteRequest{
		Term:         0,
		CandidateID:  "node-2",
		LastLogIndex: -1,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Fatalf("expected stale term vote to be rejected")
	}
	if resp.Term != 1 {
		t.Fatalf("expected response term 1, got %d", resp.Term)
	}
}

func TestHandleVoteRequestRejectsOutdatedLog(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})
	node.log = append(node.log, Entry{Term: 4, Operation: "set", Key: "pet", Value: "cat"})

	resp := node.HandleVoteRequest(VoteRequest{
		Term:         5,
		CandidateID:  "node-1",
		LastLogIndex: -1,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Fatalf("expected outdated candidate log to be rejected")
	}
	if node.VotedFor() != "" {
		t.Fatalf("expected no vote to be recorded, got %q", node.VotedFor())
	}
}

func TestHandleVoteResponsePromotesCandidateToLeaderOnQuorum(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})
	node.StartElection()

	node.HandleVoteResponse("node-2", VoteResponse{Term: 1, VoteGranted: true})

	if node.Role() != RoleLeader {
		t.Fatalf("expected node to become leader, got %s", node.Role())
	}
	if node.LeaderID() != "node-1" {
		t.Fatalf("expected leader id node-1, got %q", node.LeaderID())
	}
}

func TestHandleVoteResponseHigherTermStepsDownCandidate(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})
	node.StartElection()

	node.HandleVoteResponse("node-2", VoteResponse{Term: 2, VoteGranted: false})

	if node.Role() != RoleFollower {
		t.Fatalf("expected node to step down, got %s", node.Role())
	}
	if node.CurrentTerm() != 2 {
		t.Fatalf("expected term 2, got %d", node.CurrentTerm())
	}
	if node.VotedFor() != "" {
		t.Fatalf("expected vote to be cleared, got %q", node.VotedFor())
	}
}

func TestHandleAppendEntriesAcceptsHeartbeatFromHigherTermLeader(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})
	node.StartElection()

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     "node-1",
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		LeaderCommit: -1,
	})

	if !resp.Success {
		t.Fatalf("expected heartbeat to succeed")
	}
	if node.Role() != RoleFollower {
		t.Fatalf("expected follower role, got %s", node.Role())
	}
	if node.LeaderID() != "node-1" {
		t.Fatalf("expected leader node-1, got %q", node.LeaderID())
	}
	if node.CurrentTerm() != 2 {
		t.Fatalf("expected term 2, got %d", node.CurrentTerm())
	}
}

func TestHandleAppendEntriesRejectsMissingPreviousLog(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 0,
		PrevLogTerm:  1,
		Entries: []Entry{
			{Term: 1, Operation: "set", Key: "pet", Value: "cat"},
		},
		LeaderCommit: 0,
	})

	if resp.Success {
		t.Fatalf("expected append to fail when previous log is missing")
	}
}

func TestHandleAppendEntriesReplacesConflictingSuffix(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})
	node.log = []Entry{
		{Term: 1, Operation: "set", Key: "a", Value: "1"},
		{Term: 2, Operation: "set", Key: "b", Value: "old"},
	}

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node-1",
		PrevLogIndex: 0,
		PrevLogTerm:  1,
		Entries: []Entry{
			{Term: 3, Operation: "set", Key: "b", Value: "new"},
			{Term: 3, Operation: "set", Key: "c", Value: "3"},
		},
		LeaderCommit: 1,
	})

	if !resp.Success {
		t.Fatalf("expected append to succeed")
	}
	logEntries := node.Log()
	if len(logEntries) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(logEntries))
	}
	if logEntries[1].Term != 3 || logEntries[1].Value != "new" {
		t.Fatalf("expected conflicting entry to be replaced, got %+v", logEntries[1])
	}
	if logEntries[2].Key != "c" {
		t.Fatalf("expected appended entry for key c, got %+v", logEntries[2])
	}
}

func TestHandleAppendEntriesAdvancesCommitIndexToLeaderBound(t *testing.T) {
	node := NewStateMachine("node-2", []string{"node-1", "node-3"})
	node.log = []Entry{
		{Term: 1, Operation: "set", Key: "a", Value: "1"},
		{Term: 1, Operation: "set", Key: "b", Value: "2"},
	}

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		LeaderCommit: 5,
	})

	if !resp.Success {
		t.Fatalf("expected append to succeed")
	}
	if node.CommitIndex() != 1 {
		t.Fatalf("expected commit index 1, got %d", node.CommitIndex())
	}
}

func TestAppendCommandRequiresLeaderRole(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})

	if _, _, err := node.AppendCommand("set", "pet", "cat"); err == nil {
		t.Fatalf("expected append command to fail for follower")
	}
}

func TestAppendCommandAppendsEntryForLeader(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})
	node.StartElection()
	node.HandleVoteResponse("node-2", VoteResponse{Term: 1, VoteGranted: true})

	entry, index, err := node.AppendCommand("set", "pet", "cat")
	if err != nil {
		t.Fatalf("append command: %v", err)
	}
	if index != 0 {
		t.Fatalf("expected index 0, got %d", index)
	}
	if entry.Term != 1 || entry.Key != "pet" || entry.Value != "cat" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

func TestAdvanceCommitIndexAndTakeCommittedEntries(t *testing.T) {
	node := NewStateMachine("node-1", []string{"node-2", "node-3"})
	node.StartElection()
	node.HandleVoteResponse("node-2", VoteResponse{Term: 1, VoteGranted: true})

	if _, _, err := node.AppendCommand("set", "pet", "cat"); err != nil {
		t.Fatalf("append first command: %v", err)
	}
	if _, _, err := node.AppendCommand("set", "color", "blue"); err != nil {
		t.Fatalf("append second command: %v", err)
	}

	node.AdvanceCommitIndex(1)
	entries := node.TakeCommittedEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 committed entries, got %d", len(entries))
	}
	if node.LastApplied() != 1 {
		t.Fatalf("expected last applied 1, got %d", node.LastApplied())
	}

	entries = node.TakeCommittedEntries()
	if len(entries) != 0 {
		t.Fatalf("expected no duplicate committed entries, got %d", len(entries))
	}
}
