package com.grok.raft.core.internal

import munit.FunSuite
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*

class LeaderIndexTrackingSpec extends FunSuite {

  val addrA = TestData.addr1
  val addrB = TestData.addr2
  val addrC = TestData.addr3

  val clusterConfig = ClusterConfiguration(
    currentNode = Leader(address = addrA, currentTerm = 1L),
    members = List(addrA, addrB, addrC)
  )

  val logState = LogState(lastLogIndex = 4L, lastLogTerm = Some(2L), appliedLogIndex = 3L)

  test("Leader.onLogRequestResponse should update sentIndexMap and ackIndexMap on success") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 5L, addrC -> 3L),
      ackIndexMap = Map(addrB -> 3L, addrC -> 2L)
    )
    val response = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 8L, success = true)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    assertEquals(updatedLeader.sentIndexMap(addrB), 8L) // ackLogIndex
    assertEquals(updatedLeader.ackIndexMap(addrB), 8L)
    assertEquals(updatedLeader.sentIndexMap(addrC), 3L) // unchanged
    assertEquals(updatedLeader.ackIndexMap(addrC), 2L)  // unchanged

    assertEquals(actions.length, 1)
    assert(actions.head.isInstanceOf[CommitLogs])
    val commitAction = actions.head.asInstanceOf[CommitLogs]
    assertEquals(commitAction.ackIndexMap(addrB), 8L)
    assertEquals(commitAction.ackIndexMap(addrA), logState.appliedLogIndex)
  }

  test("Leader.onLogRequestResponse should decrement sentIndex on failure and retry replication") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 5L),
      ackIndexMap = Map(addrB -> 3L)
    )
    val response = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    assertEquals(updatedLeader.sentIndexMap(addrB), 4L) // decremented from 5 to 4
    assertEquals(updatedLeader.ackIndexMap(addrB), 3L)  // unchanged on failure

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ReplicateLog(addrB, 1L, 4L))
  }

  test("Leader.onLogRequestResponse should handle backtracking to -1 correctly") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> 0L),
      ackIndexMap = Map(addrB -> -1L)
    )
    val response = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    assertEquals(updatedLeader.sentIndexMap(addrB), -1L) // decremented to -1
    assertEquals(updatedLeader.ackIndexMap(addrB), -1L)  // unchanged

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ReplicateLog(addrB, 1L, -1L))
  }

  test("Leader.onLogRequestResponse should handle backtracking when sentIndex is already -1") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map(addrB -> -1L),
      ackIndexMap = Map(addrB -> -1L)
    )
    val response = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    assertEquals(updatedLeader.sentIndexMap(addrB), -1L) // stays at -1
    assertEquals(updatedLeader.ackIndexMap(addrB), -1L)  // unchanged

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ReplicateLog(addrB, 1L, -1L))
  }

  test("Leader.onLogRequestResponse should step down on higher term from follower") {
    val leader = Leader(
      address = addrA,
      currentTerm = 2L,
      sentIndexMap = Map(addrB -> 5L),
      ackIndexMap = Map(addrB -> 3L)
    )
    val response = LogRequestResponse(addrB, currentTerm = 3L, ackLogIndex = 5L, success = true)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Follower])
    val follower = newNode.asInstanceOf[Follower]
    assertEquals(follower.currentTerm, 3L)
    assertEquals(follower.address, addrA)
    assertEquals(follower.votedFor, None)

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ResetLeaderAnnouncer)
  }

  test("Leader.onLogRequestResponse should handle missing node in sentIndexMap") {
    val leader = Leader(
      address = addrA,
      currentTerm = 1L,
      sentIndexMap = Map.empty[NodeAddress, Long], // empty map
      ackIndexMap = Map.empty[NodeAddress, Long]
    )
    val response = LogRequestResponse(addrB, currentTerm = 1L, ackLogIndex = 0L, success = false)

    val (newNode, actions) = leader.onLogRequestResponse(logState, clusterConfig, response)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    assertEquals(updatedLeader.sentIndexMap(addrB), -1L) // defaults to -1 when not found
    assert(!updatedLeader.ackIndexMap.contains(addrB))   // not updated on failure

    assertEquals(actions.length, 2)
    assertEquals(actions(0), StoreState)
    assertEquals(actions(1), ReplicateLog(addrB, 1L, -1L))
  }

  test("Leader.onVoteRequest should update index maps when rejecting vote but helping candidate catch up") {
    val leader = Leader(
      address = addrA,
      currentTerm = 3L,
      sentIndexMap = Map(addrB -> 2L),
      ackIndexMap = Map(addrB -> 1L)
    )
    val voteRequest = VoteRequest(
      proposedLeaderAddress = addrB,
      candidateTerm = 2L, // lower term, will be rejected
      candidateLogIndex = 4L,
      candidateLastLogTerm = 2L
    )

    val (newNode, (response, actions)) = leader.onVoteRequest(voteRequest, logState, clusterConfig)

    assert(newNode.isInstanceOf[Leader])
    val updatedLeader = newNode.asInstanceOf[Leader]

    // Should update tracking maps with candidate's last index
    assertEquals(updatedLeader.sentIndexMap(addrB), 4L)
    assertEquals(updatedLeader.ackIndexMap(addrB), 4L)

    assertEquals(response.voteGranted, false)
    assertEquals(response.term, 3L) // leader's current term

    assertEquals(actions.length, 1)
    assertEquals(actions(0), ReplicateLog(addrB, 3L, 4L)) // help candidate catch up
  }

  test("Leader.onReplicateLog should generate replication actions for all followers") {
    val leader = Leader(
      address = addrA,
      currentTerm = 2L,
      sentIndexMap = Map(addrB -> 5L, addrC -> 3L),
      ackIndexMap = Map(addrB -> 4L, addrC -> 2L)
    )

    val actions = leader.onReplicateLog(clusterConfig)

    assertEquals(actions.length, 2) // for addrB and addrC, not self

    val replicateActions = actions.map(_.asInstanceOf[ReplicateLog])
    val actionMap        = replicateActions.map(a => a.peerId -> a).toMap

    assertEquals(actionMap(addrB).term, 2L)
    assertEquals(actionMap(addrB).prefixIndex, 5L)
    assertEquals(actionMap(addrC).term, 2L)
    assertEquals(actionMap(addrC).prefixIndex, 3L)
  }

  test("Leader.onReplicateLog should handle missing entries in sentIndexMap") {
    val leader = Leader(
      address = addrA,
      currentTerm = 2L,
      sentIndexMap = Map(addrB -> 5L), // missing addrC
      ackIndexMap = Map(addrB -> 4L)
    )

    val actions = leader.onReplicateLog(clusterConfig)

    assertEquals(actions.length, 2)

    val replicateActions = actions.map(_.asInstanceOf[ReplicateLog])
    val actionMap        = replicateActions.map(a => a.peerId -> a).toMap

    assertEquals(actionMap(addrB).prefixIndex, 5L)
    assertEquals(actionMap(addrC).prefixIndex, -1L) // default when missing
  }
}
