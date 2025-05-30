package com.grok.raft.core
import com.grok.raft.core.internal.*

case class ClusterConfiguration(
    currentNode: Node,
    members: List[NodeAddress] = List.empty[NodeAddress],
    followerAcceptRead: Boolean = true,
    logCompactionThreshold: Int = 100,
    electionMinDelayMillis: Int = 150,
    electionMaxDelayMillis: Int = 300,
    heartbeatIntervalMillis: Int = 2000,
    heartbeatTimeoutMillis: Int = 6000
) {
  def nodeId              = currentNode.address.id
  def nodes: List[String] = members.map(_.id)

  var quorumSize: Int = members.size / 2 + 1
}
