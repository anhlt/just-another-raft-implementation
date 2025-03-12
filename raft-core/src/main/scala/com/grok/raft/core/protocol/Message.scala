package com.grok.raft.core.protocol

import com.grok.raft.core.internal.Node
import com.grok.raft.core.internal.NodeAddress

trait Action

case class RequestForVote(peerId: NodeAddress, request: VoteRequest) extends Action

/**
    * Initiates the replication of log entries to a specified follower in the cluster.
    *
    * This action encapsulates the logic to distribute a new log entry to a follower.
    * It also triggers periodic log replication to maintain consistency and serves as a
    * heartbeat mechanism across cluster nodes. Repeatedly sending log entries ensures
    * that transient communication failures do not permanently disrupt replication.
    *
    * @param peerId   the address of the follower node to which the log entries are sent
    * @param term     the current term identifier, which is vital for maintaining consistency in the Raft protocol
    * @param nextIndex the index of the next log entry that should be replicated to the follower
    */
    // TODO: Figuringout this should be lenght or index, Maybe in Raft, we will construct the entries using nextIndex
case class ReplicateLog(peerId: NodeAddress, term: Long, nextIndex: Long) extends Action

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
