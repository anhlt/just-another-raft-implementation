package com.grok.raft.core.internal

case class ClusterConfiguration(
    currentNode: Node,
    members: List[NodeAddress] = List.empty[NodeAddress]
) {
  def nodeId              = currentNode.address.id
  def nodes: List[String] = members.map(_.id)

  var quorumSize: Int = members.size / 2 + 1
}
