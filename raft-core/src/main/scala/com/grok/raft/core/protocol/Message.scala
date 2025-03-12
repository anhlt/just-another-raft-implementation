package com.grok.raft.core.protocol

import com.grok.raft.core.internal.Node
import com.grok.raft.core.internal.NodeAddress

trait Action

case class RequestForVote(peerId: NodeAddress, request: VoteRequest) extends Action

/**
 * Initiates log replication to the specified follower node.
 *
 * The leader uses this action to synchronize a follower's log with its own,
 * ensuring consistency across the cluster. It sends log entries starting from
 * the specified prefixLength, along with periodic heartbeats to maintain node
 * connectivity.
 *
 * Parameters:
 *   - peerId: The network address of the follower.
 *   - term: The current term of the leader, validating the log replication.
 *   - prefixLength: The log index that indicates where replication should begin.
 *
 * Example:
 *   If a follower's log length is 5 and the leader's log length is 10, the leader
 *   uses this action to send the log entries beginning at index 5.
 */
case class ReplicateLog(peerId: NodeAddress, term: Long, prefixLength: Long) extends Action


case class CommitLogs(matchIndex: Map[NodeAddress, Long])                        extends Action
case class AnnounceLeader(leaderId: NodeAddress, resetPrevious: Boolean = false) extends Action

/**
 * Action to reset the leader announcer.
 *
 * This action is issued to reinitialize the state of the leader announcer, enabling
 * the leader election process to commence afresh. Typically, this is triggered when
 * the current leader is superseded, such as when a node receives a vote request with a
 * higher term from a follower.
 */
case object ResetLeaderAnnouncer extends Action


/**
 * Represents the action of persisting the current node state to storage.
 *
 * This operation is executed prior to any further processing to ensure that the node's
 * state is safely stored and can be recovered in case of failures.
 *
 * @note This action is critical for maintaining consistency and durability in the system.
 */
case object StoreState extends Action
