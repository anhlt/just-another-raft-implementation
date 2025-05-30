package com.grok.raft.core.internal


import munit.FunSuite

import com.grok.raft.core.internal._
import com.grok.raft.core.protocol._

class NodeSuite extends FunSuite {

  //–– Helpers and fixtures ––//

  val addrA = TestData.addr1
  val addrB = TestData.addr2
  val addrC = TestData.addr3

  def emptyLogState = LogState(logLength = 0L, lastLogTerm = None)
  def smallLogState  = LogState(logLength = 4L, lastLogTerm = Some(2L))

  // Create a “dummy” follower to pass into ClusterConfiguration
  private val dummyFollower = Follower(address = addrA, currentTerm = 0L)

  // Cluster with three nodes; default quorumSize = 2
  val clusterConfig = {
    val cfg = ClusterConfiguration(currentNode = dummyFollower, members = List(addrA, addrB, addrC))
    // cfg.quorumSize is computed as members.size/2 + 1 = 2
    cfg
  }


  test("Follower.onTimer should start election when no leader") {
    val follower = Follower(address = addrA, currentTerm = 1L, currentLeader = None)
    val (nextNode, actions) = follower.onTimer(emptyLogState, clusterConfig)

    // transitioned to Candidate
    assert(nextNode.isInstanceOf[Candidate])
    val cand = nextNode.asInstanceOf[Candidate]
    assertEquals(cand.currentTerm, 2L)
    assertEquals(cand.votedFor, Some(addrA))

    // first action is StoreState
    assertEquals(actions.head, StoreState)
    // then a RequestForVote to each other member
    val votePeers = actions.collect { case RequestForVote(p, _) => p }
    assertEquals(votePeers.toSet, Set(addrB, addrC))
  }

  test("Follower.onTimer should prepend ResetLeaderAnnouncer if there was a leader") {
    val follower = Follower(address = addrA, currentTerm = 5L, currentLeader = Some(addrB))
    val (_, actions) = follower.onTimer(emptyLogState, clusterConfig)

    assertEquals(actions.head, ResetLeaderAnnouncer)
  }


  test("Candidate.onTimer should increment term and send vote requests") {
    val cand = Candidate(address = addrA, currentTerm = 10L)
    val (nextNode, actions) = cand.onTimer(smallLogState, clusterConfig)

    assert(nextNode.isInstanceOf[Candidate])
    val updated = nextNode.asInstanceOf[Candidate]
    assertEquals(updated.currentTerm, 11L)
    assertEquals(updated.votedFor, Some(addrA))

    assertEquals(actions.head, StoreState)
    val peers = actions.collect { case RequestForVote(p, _) => p }
    assertEquals(peers.toSet, Set(addrB, addrC))
  }


  test("Candidate.onVoteRequest grants vote when log & term are up-to-date") {
    val cand = Candidate(address = addrA, currentTerm = 1L)
    val voteReq = VoteRequest(
      proposedLeaderAddress = addrB,
      candidateTerm         = 2L,
      candidateLogLength    = 5L,
      candidateLastLogTerm  = 3L
    )

    val (nextNode, (resp, actions)) =
      cand.onVoteRequest(voteReq, smallLogState, clusterConfig)

    // steps down to follower
    assert(nextNode.isInstanceOf[Follower])
    val fol = nextNode.asInstanceOf[Follower]
    assertEquals(fol.currentTerm, 2L)
    assertEquals(fol.votedFor, Some(addrB))

    // response granted
    assertEquals(resp.voteGranted, true)
    assertEquals(resp.term, 2L)

    // only StoreState action
    assertEquals(actions, List(StoreState))
  }

  test("Candidate.onVoteRequest rejects stale term or log") {
    val cand = Candidate(address = addrA, currentTerm = 5L)
    // lower term => automatic reject
    val badReq = VoteRequest(addrB, candidateTerm = 3L, candidateLogLength = 100L, candidateLastLogTerm = 10L)

    val (nextNode, (resp, actions)) =
      cand.onVoteRequest(badReq, smallLogState, clusterConfig)

    // stays candidate
    assert(nextNode.isInstanceOf[Candidate])
    assertEquals(resp.voteGranted, false)
    assertEquals(resp.term, 5L)
    assert(actions.isEmpty)
  }


  test("Candidate.onVoteResponse becomes leader when quorum reached") {
    // initial state: voted for self, one vote in hand
    val start = Candidate(address = addrA, currentTerm = 1L, votedFor = Some(addrA), voteReceived = Set(addrA))

    // receive vote from B
    val rB = VoteResponse(addrB, term = 1L, voteGranted = true)
    val (midState, act1) = start.onVoteResponse(rB, emptyLogState, clusterConfig)
    // still candidate, no actions
    assert(midState.isInstanceOf[Leader])
    assert(act1(0) == StoreState)
    assert(act1(1) == AnnounceLeader(addrA))

    val peers = act1.collect { case ReplicateLog(p, _, _) => p }.toSet
    assertEquals(peers, Set(addrB, addrC))

    // receive vote from C => quorum of 2
    val rC = VoteResponse(addrC, term = 1L, voteGranted = true)
    val (leaderState, act2) = midState.onVoteResponse(rC, emptyLogState, clusterConfig)

    assert(leaderState.isInstanceOf[Leader])
    // actions: StoreState, AnnounceLeader, then ReplicateLog to each member
    assert(act2.isEmpty)
  }


  test("Follower.onLogRequest accepts when term >= and log consistent") {
    val fol = Follower(address = addrA, currentTerm = 1L, currentLeader = Some(addrC))
    val entries = List(LogEntry(term = 2L, index = 1L, command = NoOp))
    val req = LogRequest(
      leaderId          = addrB,
      term              = 1L,
      prevSentLogLength = 0L,
      prevLastLogTerm   = 0L,
      entries           = entries,
      leaderCommit      = 0L
    )

    val (nextNode, (resp, actions)) = fol.onLogRequest(req, emptyLogState, None, clusterConfig)

    // stays follower with updated term/leader
    assert(nextNode.isInstanceOf[Follower])
    val updated = nextNode.asInstanceOf[Follower]
    assertEquals(updated.currentTerm, 1L)
    assertEquals(updated.currentLeader, Some(addrB))

    assert(resp.success)
    // actions: StoreState, AnnounceLeader
    assertEquals(actions, List(StoreState, AnnounceLeader(leaderId = addrB, resetPrevious = true)))
  }

  test("Follower.onLogRequest rejects when term is stale") {
    val fol = Follower(address = addrA, currentTerm = 5L, currentLeader = None)
    val req = LogRequest(
      leaderId          = addrB,
      term              = 3L, // stale
      prevSentLogLength = 0L,
      prevLastLogTerm   = 0L,
      entries           = Nil,
      leaderCommit      = 0L
    )

    val (nextNode, (resp, actions)) = fol.onLogRequest(req, emptyLogState, None, clusterConfig)

    // no state change
    assert(nextNode == fol)
    assert(!resp.success)
    assert(actions.isEmpty)
  }

}