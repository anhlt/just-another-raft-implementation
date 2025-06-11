package com.grok.raft.core.storage

import com.grok.raft.core.internal.*

case class PersistedState(term: Long, votedFor: Option[NodeAddress], appliedIndex: Long = 0L):
  def toNodeState(nodeAddress: NodeAddress): Node=
    Follower(address=nodeAddress, currentTerm = term, votedFor = votedFor)
