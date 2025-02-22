package com.grok.raft.core.internal


case class AppendEntriesRequest(
	leaderId: NodeAddress,
	term: Long,
	prevLogIndex: Long,
	prevLogTerm: Long,
	learCommit: Long,
	entries: List[LogEntry]
)