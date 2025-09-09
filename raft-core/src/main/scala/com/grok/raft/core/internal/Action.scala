package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/**
 * Action is outcome of NodeState transitions.
 * It represents the actions that can be taken by the Raft node
 * in response to state changes, such as sending requests, committing logs,
 * or announcing leadership. Each action corresponds to a specific
 * operation that the node can perform in the Raft consensus algorithm.
*/
trait Action

case class RequestForVote(peerId: NodeAddress, request: VoteRequest) extends Action

/**
 * Initiates log replication to the specified follower node.
 *
 * The leader uses this action to synchronize a follower's log with its own,
 * ensuring consistency across the cluster. It sends log entries starting from
 * the specified prefixIndex, along with periodic heartbeats to maintain node
 * connectivity.
 *
 * Parameters:
 *   - peerId: The network address of the follower.
 *   - term: The current term of the leader, validating the log replication.
 *   - prefixIndex: The log index (0-based) that indicates the last sent entry.
 *
 * Example:
 *   If a follower's last acknowledged index is 4, the leader uses this action
 *   to send the log entries beginning at index 5.
 */
case class ReplicateLog(peerId: NodeAddress, term: Long, prefixIndex: Long) extends Action


case class CommitLogs(ackIndexMap: Map[NodeAddress, Long])                        extends Action


/**
 * Action to announce a leader to the cluster.
 *
 * This action is used by the leader node to notify other nodes of its leadership status.
 * It includes the leader's ID and an optional flag to reset any previous announcements.
 *
 * @param leaderId The identifier of the leader node.
 * @param resetPrevious If true, resets any previous leader announcements.
 */
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
