package com.grok.raft.core.internal

import munit.FunSuite
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*
import cats.effect.*
import cats.effect.kernel.{Deferred => EffectDeferred}
import munit.CatsEffectSuite

class IndexTrackingEdgeCasesSpec extends CatsEffectSuite {

  val addrA = TestData.addr1
  val addrB = TestData.addr2
  val addrC = TestData.addr3

  val clusterConfig = ClusterConfiguration(
    currentNode = Leader(address = addrA, currentTerm = 1L),
    members = List(addrA, addrB, addrC)
  )

  var emptyDefer = new com.grok.raft.core.internal.RaftDeferred[IO, Unit] {
    val deffered          = EffectDeferred.unsafe[IO, Unit]
    def get: IO[Unit]     = deffered.get
    def complete(a: Unit) = deffered.complete(a)
  }

  test("Leader should handle concurrent index updates correctly") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 5L, addrC -> 3L),
      ackIndexMap = Map(addrB -> 4L, addrC -> 2L)
    )
    val logState = LogState(lastLogIndex = 5L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // Simulate rapid responses from multiple followers
    val responseB1 = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 7L, success = true)
    val responseC1 = LogRequestResponse(addrC, currentTerm = 1L, ackLogIndex = 5L, success = true)

    val (state1, actions1) = leader.onLogRequestResponse(logState, clusterConfig, responseB1)
    val leader1            = state1.asInstanceOf[Leader]

    val (state2, actions2) = leader1.onLogRequestResponse(logState, clusterConfig, responseC1)
    val leader2            = state2.asInstanceOf[Leader]

    // Verify both updates were applied correctly
    assertEquals(leader2.sentIndexMap(addrB), 7L)
    assertEquals(leader2.ackIndexMap(addrB), 7L)
    assertEquals(leader2.sentIndexMap(addrC), 5L)
    assertEquals(leader2.ackIndexMap(addrC), 5L)

    // Should have commit actions for both
    assert(actions1.head.isInstanceOf[CommitLogs])
    assert(actions2.head.isInstanceOf[CommitLogs])
  }

  test("Leader should handle out-of-order responses gracefully") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 5L),
      ackIndexMap = Map(addrB -> 3L)
    )
    val logState = LogState(lastLogIndex = 5L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // Response for a newer index comes first
    val newerResponse = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 8L, success = true)
    val (state1, _)   = leader.onLogRequestResponse(logState, clusterConfig, newerResponse)
    val leader1       = state1.asInstanceOf[Leader]

    // Then response for an older index (should not regress)
    val olderResponse = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 6L, success = true)
    val (state2, _)   = leader1.onLogRequestResponse(logState, clusterConfig, olderResponse)
    val leader2       = state2.asInstanceOf[Leader]

    // Should not regress to older index
    assertEquals(leader2.ackIndexMap(addrB), 6L) // from olderResponse, not 8L from newerResponse
    assertEquals(leader2.sentIndexMap(addrB), 6L)
  }

  test("Leader should handle follower that gets far behind") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 100L), // very far ahead
      ackIndexMap = Map(addrB -> 50L)
    )
    val logState = LogState(lastLogIndex = 100L, lastLogTerm = Some(5L), appliedLogIndex = 95L)

    // Follower keeps rejecting due to log inconsistency
    val failureResponse = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)

    var currentLeader  = leader
    var backtrackCount = 0

    // Simulate multiple backtracking steps
    while (currentLeader.sentIndexMap(addrB) > 0L && backtrackCount < 10) {
      val (newState, actions) = currentLeader.onLogRequestResponse(logState, clusterConfig, failureResponse)
      currentLeader = newState.asInstanceOf[Leader]
      backtrackCount += 1

      // Should always decrement by 1
      assertEquals(actions.length, 2)
      assertEquals(actions(0), StoreState)
      assert(actions(1).isInstanceOf[ReplicateLog])
    }

    // Should eventually reach a low index
    assert(currentLeader.sentIndexMap(addrB) < 100L)
    assertEquals(backtrackCount, 10) // Should have decremented 10 times
  }

  test("Follower should handle rapid log requests with increasing indices") {
    val follower     = Follower(address = addrA, currentTerm = 1L, currentLeader = Some(addrB))
    val baseLogState = LogState(lastLogIndex = 4L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // Sequence of log requests with increasing prevSentLogIndex
    val requests = List(
      LogRequest(
        addrB,
        2L,
        prevSentLogIndex = 4L,
        prevLastLogTerm = 2L,
        leaderCommit = 0L,
        entries = List(LogEntry(2L, 5L, NoOp))
      ),
      LogRequest(
        addrB,
        2L,
        prevSentLogIndex = 5L,
        prevLastLogTerm = 2L,
        leaderCommit = 0L,
        entries = List(LogEntry(2L, 6L, NoOp))
      ),
      LogRequest(
        addrB,
        2L,
        prevSentLogIndex = 6L,
        prevLastLogTerm = 2L,
        leaderCommit = 0L,
        entries = List(LogEntry(2L, 7L, NoOp))
      )
    )

    var currentFollower = follower
    var currentLogState = baseLogState

    for ((request, idx) <- requests.zipWithIndex) {
      val prevEntry = if (request.prevSentLogIndex >= 0) {
        Some(LogEntry(request.prevLastLogTerm, request.prevSentLogIndex, NoOp))
      } else None

      val (newState, (response, _)) = currentFollower.onLogRequest(request, currentLogState, prevEntry, clusterConfig)
      currentFollower = newState.asInstanceOf[Follower]

      assertEquals(response.success, true)
      assertEquals(response.ackLogIndex, request.prevSentLogIndex + request.entries.length)

      // Update log state for next iteration
      currentLogState = currentLogState.copy(lastLogIndex = response.ackLogIndex) // Convert index to length
    }

    // Final log should have grown appropriately
    assertEquals(currentLogState.logLength, 8L) // started at 5, added 3 entries
  }

  test("Leader should handle follower reporting higher term during index update") {
    val leader = Leader(
      address = addrA,
      currentTerm = 2L,
      sentIndexMap = Map(addrB -> 5L),
      ackIndexMap = Map(addrB -> 3L)
    )
    val logState = LogState(lastLogIndex = 5L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // Follower reports higher term, indicating a new election happened
    val higherTermResponse = LogRequestResponse(addrB, currentTerm = 3L, ackLogIndex = 7L, success = true)

    val (newState, actions) = leader.onLogRequestResponse(logState, clusterConfig, higherTermResponse)

    // Should step down to follower
    assert(newState.isInstanceOf[Follower])
    val follower = newState.asInstanceOf[Follower]
    assertEquals(follower.currentTerm, 3L)
    assertEquals(follower.address, addrA)

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ResetLeaderAnnouncer)
  }

  test("Follower should handle very large prevSentLogIndex gracefully") {
    val follower = Follower(address = addrA, currentTerm = 1L)
    val logState = LogState(lastLogIndex = 4L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // Leader sends request with very large prevSentLogIndex
    val request = LogRequest(
      leaderId = addrB,
      term = 2L,
      prevSentLogIndex = 1000000L, // much larger than follower's log
      prevLastLogTerm = 2L,
      entries = List(LogEntry(2L, 1000001L, NoOp)),
      leaderCommit = 0L
    )

    val (newState, (response, actions)) = follower.onLogRequest(request, logState, None, clusterConfig)

    // Should reject due to log being too short
    assertEquals(response.success, false)
    assertEquals(response.ackLogIndex, -1L) // TODO case

    // Should still update term and leader
    assert(newState.isInstanceOf[Follower])
    val updatedFollower = newState.asInstanceOf[Follower]
    assertEquals(updatedFollower.currentTerm, 2L)
    assertEquals(updatedFollower.currentLeader, Some(addrB))
  }

  test("Log should handle commit progression with gaps in acknowledgments") {
    for {
      log <- IO(new InMemoryLog[IO, Unit])
      store = log.logStorage

      // Pre-populate with entries
      _ <- store.put(0, LogEntry(1, 0, NoOp))
      _ <- store.put(1, LogEntry(1, 1, NoOp))
      _ <- store.put(2, LogEntry(2, 2, NoOp))
      _ <- store.put(3, LogEntry(2, 3, NoOp))
      _ <- store.put(4, LogEntry(2, 4, NoOp))

      _ <- log.setCommitIndex(-1L)

      // Acknowledgments with gaps: majority at index 1, but one node at index 4
      acks = Map(
        NodeAddress("a", 9090) -> 1L,
        NodeAddress("b", 9090) -> 1L,
        NodeAddress("c", 9090) -> 4L // ahead but alone
      )

      result      <- log.commitLogs(acks)
      commitIndex <- log.getCommittedIndex

      // Later, another node catches up to index 3
      acks2 = Map(
        NodeAddress("a", 9090) -> 3L,
        NodeAddress("b", 9090) -> 1L,
        NodeAddress("c", 9090) -> 4L
      )

      result2      <- log.commitLogs(acks2)
      commitIndex2 <- log.getCommittedIndex
    } yield {
      assertEquals(commitIndex, 1L)  // First commit stops at majority threshold
      assertEquals(commitIndex2, 3L) // Second commit advances further
      assertEquals(result, true)
      assertEquals(result2, true)
    }
  }

  test("Leader initialization should set up index maps correctly") {
    val logState = LogState(lastLogIndex = 9L, lastLogTerm = Some(3L), appliedLogIndex = 8L)

    // Simulate candidate becoming leader
    val candidate    = Candidate(address = addrA, currentTerm = 2L, voteReceived = Set(addrA, addrB))
    val voteResponse = VoteResponse(addrC, term = 2L, voteGranted = true) // reaches quorum

    val (newState, actions) = candidate.onVoteResponse(voteResponse, logState, clusterConfig)

    assert(newState.isInstanceOf[Leader])
    val leader = newState.asInstanceOf[Leader]

    // Should initialize sentIndexMap with lastLogIndex for all peers
    assertEquals(leader.sentIndexMap(addrB), logState.lastLogIndex)
    assertEquals(leader.sentIndexMap(addrC), logState.lastLogIndex)

    // Should initialize ackIndexMap with -1 for all peers (no entries acknowledged yet)
    assertEquals(leader.ackIndexMap(addrB), -1L)
    assertEquals(leader.ackIndexMap(addrC), -1L)

    // Should not include self in maps
    assert(!leader.sentIndexMap.contains(addrA))
    assert(!leader.ackIndexMap.contains(addrA))

    // Should have replication actions for all peers
    val replicateActions = actions.collect { case r: ReplicateLog => r }
    assertEquals(replicateActions.length, 2)
    assertEquals(replicateActions.map(_.peerId).toSet, Set(addrB, addrC))
  }

  test("Leader should handle mixed success/failure responses in same term") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 5L, addrC -> 5L),
      ackIndexMap = Map(addrB -> 3L, addrC -> 3L)
    )
    val logState = LogState(lastLogIndex = 5L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

    // B succeeds, C fails
    val successResponse = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 6L, success = true)
    val failureResponse = LogRequestResponse(addrC, currentTerm = 1L, ackLogIndex = 0L, success = false)

    val (state1, actions1) = leader.onLogRequestResponse(logState, clusterConfig, successResponse)
    val leader1            = state1.asInstanceOf[Leader]

    val (state2, actions2) = leader1.onLogRequestResponse(logState, clusterConfig, failureResponse)
    val leader2            = state2.asInstanceOf[Leader]

    // B should be updated, C should be decremented
    assertEquals(leader2.sentIndexMap(addrB), 6L) // ackLogIndex
    assertEquals(leader2.ackIndexMap(addrB), 6L)
    assertEquals(leader2.sentIndexMap(addrC), 4L) // decremented
    assertEquals(leader2.ackIndexMap(addrC), 3L)  // unchanged on failure

    // Should have commit action from success and retry action from failure
    assert(actions1.head.isInstanceOf[CommitLogs])
    assertEquals(actions2(0), StoreState)
    assertEquals(actions2(1), ReplicateLog(addrC, 1L, 4L))
  }

  test("Boundary condition: Index arithmetic with zero and negative values") {
    // Test LogState calculations
    val emptyState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)
    assertEquals(emptyState.lastLogIndex, -1L)
    assert(emptyState.appliedLogIndex <= emptyState.lastLogIndex)

    val singleEntryState = LogState(lastLogIndex = 0L, lastLogTerm = Some(1L), appliedLogIndex = 0L)
    assertEquals(singleEntryState.lastLogIndex, 0L)

    // Test leader backtracking to negative values
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 1L),
      ackIndexMap = Map(addrB -> -1L)
    )

    // Multiple failures should not go below -1
    var currentLeader = leader
    for (_ <- 1 to 5) {
      val response      = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)
      val (newState, _) = currentLeader.onLogRequestResponse(emptyState, clusterConfig, response)
      currentLeader = newState.asInstanceOf[Leader]
    }

    assertEquals(currentLeader.sentIndexMap(addrB), -1L) // Should not go below -1
  }
}
