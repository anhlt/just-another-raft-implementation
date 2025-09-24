package com.grok.raft.core.internal

import munit.FunSuite
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*

class FollowerIndexResponseSpec extends FunSuite {

  val addrA = TestData.addr1
  val addrB = TestData.addr2
  val addrC = TestData.addr3

  val clusterConfig = ClusterConfiguration(
    currentNode = Follower(address = addrA, currentTerm = 1L),
    members = List(addrA, addrB, addrC)
  )

  test("Follower.onLogRequest should return correct ackLogIndex on successful append") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val entries = List(
      LogEntry(term = 2L, index = 3L, command = NoOp),
      LogEntry(term = 2L, index = 4L, command = NoOp)
    )
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = 2L,
      prevLastLogTerm = 1L,
      entries = entries,
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = 2L, lastLogTerm = Some(1L), appliedLogIndex = 2L)
    val prevLogEntry = Some(LogEntry(1L, 2L, NoOp)) // consistent with prevLastLogTerm

    val (newNode, (response, actions)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assert(newNode.isInstanceOf[Follower])
    val updatedFollower = newNode.asInstanceOf[Follower]
    assertEquals(updatedFollower.currentTerm, 2L)
    assertEquals(updatedFollower.currentLeader, Some(addrB))

    assertEquals(response.success, true)
    assertEquals(response.currentTerm, 2L)
    assertEquals(response.ackLogIndex, 4L) // prevSentLogIndex + entries.length = 2 + 2
    assertEquals(response.nodeId, addrA)

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), AnnounceLeader(leaderId = addrB, resetPrevious = false))
  }

  test("Follower.onLogRequest should return correct ackLogIndex with empty entries") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = 2L,
      prevLastLogTerm = 1L,
      entries = Nil, // empty entries (heartbeat)
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = 2L, lastLogTerm = Some(1L), appliedLogIndex = 2L)
    val prevLogEntry = Some(LogEntry(1L, 2L, NoOp))

    val (_, (response, _)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assertEquals(response.success, true)
    assertEquals(response.ackLogIndex, 2L) // prevSentLogIndex + 0 = 2 + 0
  }

  test("Follower.onLogRequest should return -1 ackLogIndex on log length mismatch") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = 5L, // follower's log is too short
      prevLastLogTerm = 2L,
      entries = List(LogEntry(2L, 6L, NoOp)),
      leaderCommit = 0L
    )
    val logState =
      LogState(lastLogIndex = 2L, lastLogTerm = Some(1L), appliedLogIndex = 2L) // only 3 entries (indices 0,1,2)
    val prevLogEntry = None // no entry at index 5

    val (newNode, (response, actions)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assert(newNode.isInstanceOf[Follower])
    val updatedFollower = newNode.asInstanceOf[Follower]
    assertEquals(updatedFollower.currentTerm, 2L)
    assertEquals(updatedFollower.currentLeader, Some(addrB))

    assertEquals(response.success, false)
    assertEquals(response.ackLogIndex, -1L) // TODO case - this might need improvement
    assertEquals(response.currentTerm, 2L)

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), AnnounceLeader(leaderId = addrB, resetPrevious = false))
  }

  test("Follower.onLogRequest should return -1 ackLogIndex on term mismatch") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = 2L,
      prevLastLogTerm = 3L, // term mismatch
      entries = List(LogEntry(2L, 3L, NoOp)),
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = 2L, lastLogTerm = Some(1L), appliedLogIndex = 2L)
    val prevLogEntry = Some(LogEntry(1L, 2L, NoOp)) // term 1, but request expects term 3

    val (_, (response, _)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assertEquals(response.success, false)
    assertEquals(response.ackLogIndex, -1L)
  }

  test("Follower.onLogRequest should handle prevSentLogIndex = -1 (empty log scenario)") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = -1L, // indicates empty log
      prevLastLogTerm = 0L,
      entries = List(LogEntry(2L, 0L, NoOp)),
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L) // empty log
    val prevLogEntry = None

    val (_, (response, _)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assertEquals(response.success, true)
    assertEquals(response.ackLogIndex, 0L) // -1 + 1 = 0
  }

  test("Follower.onLogRequest should reject request from stale term") {
    val follower = Follower(address = addrA, currentTerm = 5L) // higher term
    val request = LogRequest(
      leaderId = addrB,
      term = 3L, // stale term
      prevSentLogIndex = 2L,
      prevLastLogTerm = 1L,
      entries = List(LogEntry(3L, 3L, NoOp)),
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = 2L, lastLogTerm = Some(1L), appliedLogIndex = 2L)
    val prevLogEntry = Some(LogEntry(1L, 2L, NoOp))

    val (newNode, (response, actions)) = follower.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assertEquals(newNode, follower) // no state change
    assertEquals(response.success, false)
    assertEquals(response.currentTerm, 5L)                    // follower's current term
    assertEquals(response.ackLogIndex, logState.lastLogIndex) // current last log index
    assertEquals(actions, List.empty[Action])                 // no actions
  }

  test("Follower.onLogRequest should announce new leader when currentLeader changes") {
    val follower = Follower(address = addrA, currentTerm = 1L, currentLeader = Some(addrC))
    val request = LogRequest(
      leaderId = addrB, // different leader
      term = 2L,
      prevSentLogIndex = -1L,
      prevLastLogTerm = 0L,
      entries = Nil,
      leaderCommit = 0L
    )
    val logState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)

    val (newNode, (response, actions)) = follower.onLogRequest(request, logState, None, clusterConfig)

    assert(newNode.isInstanceOf[Follower])
    val updatedFollower = newNode.asInstanceOf[Follower]
    assertEquals(updatedFollower.currentLeader, Some(addrB))

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(
      actions(1),
      AnnounceLeader(leaderId = addrB, resetPrevious = true)
    ) // resetPrevious = true because previous leader existed
  }

  test("Follower.onLogRequest should not announce leader when currentLeader stays the same") {
    val follower = Follower(address = addrA, currentTerm = 1L, currentLeader = Some(addrB))
    val request = LogRequest(
      leaderId = addrB, // same leader
      term = 2L,
      prevSentLogIndex = -1L,
      prevLastLogTerm = 0L,
      entries = Nil,
      leaderCommit = 0L
    )
    val logState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)

    val (_, (_, actions)) = follower.onLogRequest(request, logState, None, clusterConfig)

    assertEquals(actions.length, 1) // only StoreState, no AnnounceLeader
    assertEquals(actions(0), StoreState)
  }

  test("Candidate.onLogRequest should step down and return correct ackLogIndex") {
    val candidate = Candidate(address = addrA, currentTerm = 2L, votedFor = Some(addrA))
    val request = LogRequest(
      leaderId = addrB,
      term = 3L, // higher term
      prevSentLogIndex = 1L,
      prevLastLogTerm = 1L,
      entries = List(LogEntry(3L, 2L, NoOp)),
      leaderCommit = 0L
    )
    val logState     = LogState(lastLogIndex = 1L, lastLogTerm = Some(1L), appliedLogIndex = 1L)
    val prevLogEntry = Some(LogEntry(1L, 1L, NoOp))

    val (newNode, (response, actions)) = candidate.onLogRequest(request, logState, prevLogEntry, clusterConfig)

    assert(newNode.isInstanceOf[Follower])
    val follower = newNode.asInstanceOf[Follower]
    assertEquals(follower.currentTerm, 3L)
    assertEquals(follower.currentLeader, Some(addrB))

    assertEquals(response.success, true)
    assertEquals(response.currentTerm, 3L)
    assertEquals(response.ackLogIndex, 2L) // 1 + 1

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), AnnounceLeader(addrB))
  }

  test("Candidate.onLogRequest should reject stale term request") {
    val candidate = Candidate(address = addrA, currentTerm = 5L)
    val request = LogRequest(
      leaderId = addrB,
      term = 3L, // stale term
      prevSentLogIndex = 1L,
      prevLastLogTerm = 1L,
      entries = Nil,
      leaderCommit = 0L
    )
    val logState = LogState(lastLogIndex = 1L, lastLogTerm = Some(1L), appliedLogIndex = 1L)

    val (newNode, (response, actions)) = candidate.onLogRequest(request, logState, None, clusterConfig)

    assertEquals(newNode, candidate) // no state change
    assertEquals(response.success, false)
    assertEquals(response.currentTerm, 5L) // candidate's current term
    assertEquals(response.ackLogIndex, logState.lastLogIndex)
    assertEquals(actions, List.empty[Action])
  }
}
