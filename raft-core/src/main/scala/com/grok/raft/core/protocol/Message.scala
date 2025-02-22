package com.grok.raft.core.protocol

import com.grok.raft.core.internal.Node
import com.grok.raft.core.internal.NodeAddress

trait Action

case class RequestForVote(peerId: NodeAddress, request: VoteRequest)
    extends Action
case class ReplicateLog(peerId: NodeAddress, term: Long, nextIndex: Long)
    extends Action
case class CommitLogs(matchIndex: Map[NodeAddress, Long]) extends Action
case class AnnounceLeader(leaderId: NodeAddress, resetPrevious: Boolean = false)
    extends Action
case object ResetLeaderAnnouncer extends Action


//  The action for storing the current node state to persistence memory before doing anything else
case object StoreState extends Action
