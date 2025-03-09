package com.grok.raft.core.protocol

import com.grok.raft.core.internal.Node
import com.grok.raft.core.internal.NodeAddress

trait Action

case class RequestForVote(peerId: NodeAddress, request: VoteRequest)
    extends Action



/*
    Does the actual work of taking the new entry in the log and distributing it to the followers
    Periodically send the logs to the followers
        It also act as a heartbeat to the followers
        Sometimes messages can be lost, so it is important to keep sending the logs

*/
case class ReplicateLog(peerId: NodeAddress, term: Long, nextIndex: Long)
    extends Action



case class CommitLogs(matchIndex: Map[NodeAddress, Long]) extends Action
case class AnnounceLeader(leaderId: NodeAddress, resetPrevious: Boolean = false)
    extends Action
case object ResetLeaderAnnouncer extends Action


//  The action for storing the current node state to persistence memory before doing anything else
case object StoreState extends Action
