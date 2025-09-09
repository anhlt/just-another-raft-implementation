package com.grok.raft.core.internal
/**
 * Log replication request from leader to follower in the Raft consensus algorithm.
 * 
 * Sent by the leader under these conditions:
 * - After leader election (as initial heartbeat)
 * - Periodically as heartbeats to prevent election timeouts
 * - When new log entries need replication
 *
 * @param leaderId          Leader's unique identifier
 * @param term              Leader's current term
 * @param prevSentLogIndex  Index of the last log entry previously sent to this follower (0-based)
 * @param prevLastLogTerm   Term of the log entry preceding new entries
 * @param leaderCommit      Leader's last committed log index
 * @param entries           Log entries to replicate
 *
 * Leaders track sent entries using a sentIndex mapping for each follower.
 * prevLastLogTerm enables followers to verify log consistency.
 */
case class LogRequest(
    val leaderId: NodeAddress,
    val term: Long,
    val prevSentLogIndex: Long,
    val prevLastLogTerm: Long,
    val leaderCommit: Long,
    val entries: List[LogEntry]
)
