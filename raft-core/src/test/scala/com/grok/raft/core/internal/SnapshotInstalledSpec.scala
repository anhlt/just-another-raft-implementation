package com.grok.raft.core.internal

import cats.effect.IO
import munit.CatsEffectSuite
import com.grok.raft.core.protocol.LogRequestResponse
import com.grok.raft.core.ClusterConfiguration

class SnapshotInstalledSpec extends CatsEffectSuite {

  val clusterConfig = ClusterConfiguration(
    currentNode = TestData.leader,
    members = List(TestData.addr1, TestData.addr2, TestData.addr3)
  )
  
  val logState = LogState(lastLogIndex = 4L, lastLogTerm = Some(2L), appliedLogIndex = 4L)

  test("Follower.onSnapshotInstalled should return positive acknowledgment") {
    val follower = Follower(
      currentTerm = 2L,
      address = TestData.addr2,
      currentLeader = Some(TestData.addr1),
      votedFor = None
    )

    val (newNode, response) = follower.onSnapshotInstalled(logState, clusterConfig)

    // Should remain a follower
    assert(newNode.isInstanceOf[Follower])
    val newFollower = newNode.asInstanceOf[Follower]
    assertEquals(newFollower.currentTerm, 2L)
    assertEquals(newFollower.address, TestData.addr2)
    assertEquals(newFollower.currentLeader, Some(TestData.addr1))
    assertEquals(newFollower.votedFor, None)

    // Should return positive response
    val expectedResponse = LogRequestResponse(
      nodeId = TestData.addr2,
      currentTerm = 2L,
      ackLogIndex = logState.lastLogIndex,
      success = true
    )
    assertEquals(response, expectedResponse)
  }

  test("Candidate.onSnapshotInstalled should step down to Follower") {
    val candidate = Candidate(
      currentTerm = 3L,
      address = TestData.addr3,
      votedFor = Some(TestData.addr3)
    )

    val (newNode, response) = candidate.onSnapshotInstalled(logState, clusterConfig)

    // Should step down to follower
    assert(newNode.isInstanceOf[Follower])
    val newFollower = newNode.asInstanceOf[Follower]
    assertEquals(newFollower.currentTerm, 3L)
    assertEquals(newFollower.address, TestData.addr3)
    assertEquals(newFollower.currentLeader, None) // Will be set on next log request
    assertEquals(newFollower.votedFor, Some(TestData.addr3)) // Preserves vote from candidate state

    // Should return positive response
    val expectedResponse = LogRequestResponse(
      nodeId = TestData.addr3,
      currentTerm = 3L,
      ackLogIndex = logState.lastLogIndex,
      success = true
    )
    assertEquals(response, expectedResponse)
  }

  test("Leader.onSnapshotInstalled should acknowledge but maintain leader state") {
    val leader = Leader(
      currentTerm = 4L,
      address = TestData.addr1,
      sentIndexMap = Map.empty,
      ackIndexMap = Map.empty
    )

    val (newNode, response) = leader.onSnapshotInstalled(logState, clusterConfig)

    // Should remain a leader (though this is unusual)
    assert(newNode.isInstanceOf[Leader])
    val newLeader = newNode.asInstanceOf[Leader]
    assertEquals(newLeader.currentTerm, 4L)
    assertEquals(newLeader.address, TestData.addr1)
    assertEquals(newLeader.sentIndexMap, Map.empty)
    assertEquals(newLeader.ackIndexMap, Map.empty)

    // Should return positive response
    val expectedResponse = LogRequestResponse(
      nodeId = TestData.addr1,
      currentTerm = 4L,
      ackLogIndex = logState.lastLogIndex,
      success = true
    )
    assertEquals(response, expectedResponse)
  }

  test("All node types should acknowledge with lastLogIndex") {
    val nodes = List(
      TestData.follower1.copy(currentTerm = 5L),
      Candidate(currentTerm = 5L, address = TestData.addr2, votedFor = Some(TestData.addr2)),
      Leader(currentTerm = 5L, address = TestData.addr3, Map.empty, Map.empty)
    )

    val customLogState = LogState(lastLogIndex = 9L, lastLogTerm = Some(3L), appliedLogIndex = 8L)

    nodes.foreach { node =>
      val (_, response) = node.onSnapshotInstalled(customLogState, clusterConfig)
      
      assertEquals(response.currentTerm, 5L)
      assertEquals(response.ackLogIndex, customLogState.lastLogIndex)
      assertEquals(response.success, true)
      assertEquals(response.nodeId, node.address)
    }
  }

  test("onSnapshotInstalled should work with empty log state") {
    val emptyLogState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)
    val follower = TestData.follower1.copy(currentTerm = 1L)

    val (_, response) = follower.onSnapshotInstalled(emptyLogState, clusterConfig)

    assertEquals(response.ackLogIndex, -1L) // Should be lastLogIndex of empty log
    assertEquals(response.success, true)
  }
}