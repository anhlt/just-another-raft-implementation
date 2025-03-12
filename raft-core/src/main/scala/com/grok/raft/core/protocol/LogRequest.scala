package com.grok.raft.core.internal
/**
  * Log replication request from the leader to a follower.
  *
  * Generated when a new log entry is appended or periodically when no new entries are available.
  *
  * @param leaderId          Unique identifier of the leader node.
  * @param term              Current term of the leader.
  * @param prevSentLogLength Count of log entries previously sent to the follower.
  * @param prevLastLogTerm   Term of the log entry immediately preceding the new entries,
  *                          used for consistency verification.
  * @param leaderCommit      Index of the last committed log entry on the leader.
  * @param entries           List of log entries to replicate.
  *
  * The leader maintains a mapping (sentLength) that tracks the number of log entries sent to each follower.
  * To construct the log request:
  *   - Retrieve prevSentLogLength from the sentLength mapping using the follower's identifier.
  *   - Obtain prevLastLogTerm from the log entry at the index equal to prevSentLogLength.
  *   - Select the entries from the range [prevSentLogLength, logLength - 1] for replication.
  *
  * Including prevLastLogTerm enables the follower to verify the consistency of its log.
  */
case class LogRequest(
    leaderId: NodeAddress,
    term: Long,
    prevSentLogLength: Long,
    prevLastLogTerm: Long,
    leaderCommit: Long,
    entries: List[LogEntry]
)
