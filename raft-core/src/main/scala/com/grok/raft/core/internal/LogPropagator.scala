package com.grok.raft.core.internal

import com.grok.raft.core.protocol._

/**
 * Responsible for propagating (replicating) log entries from this server (the leader)
 * to a follower via the AppendEntries RPC. This is used both for:
 *   1. Replicating new entries when clients submit commands.
 *   2. Sending periodic heartbeats (empty entries) to maintain leadership.
 *
 * Internally, `propagateLogs` will:
 *   1. Fetch the term of the log entry at (nextIndex - 1), defaulting to 0 if none.
 *   2. Retrieve all log entries from `nextIndex` through the end of the local log.
 *   3. Read the current commit index from the `Log` interface.
 *   4. Construct a `LogRequest` payload with:
 *        - leaderId (this node’s ID)
 *        - current term
 *        - prevSentLogLength = nextIndex - 1
 *        - prevLastLogTerm  = term of that previous entry
 *        - leaderCommit     = current commit index
 *        - entries          = the list of fetched LogEntry objects
 *   5. Send the `LogRequest` to the specified peer and return its `LogRequestResponse`.
 *
 * @tparam F effect type (e.g. IO, Task, etc.)
 */
trait LogPropagator[F[_]]:
	def propagateLogs(peerId: NodeAddress, term: Long, nextIndex: Long): F[LogRequestResponse]	
