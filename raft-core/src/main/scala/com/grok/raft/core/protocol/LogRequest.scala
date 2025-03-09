package com.grok.raft.core.internal



/**
  * Request to replicate the logs from leader to followers. 
  * Sent from leader whenever there is a new message in the log and also periodically
  * If there is no message the entries is empty list
  * 
  * Leader has a dictinary sentLength which keeps track of the number of logs sent to each follower
  * To construct the log request, leader get the prevLogIndex from the sentLength and get the logs from the log
  * 
  * prevLogIndex = sentLenght[followerID]
  * 
  * entries = log[prevLogIndex:]
  * 
  * it also need to send the prevLogTerm to the follower so the follower can verify the logs consistency
  * 
  */
case class LogRequest(
	leaderId: NodeAddress,
	term: Long,
	prevLogIndex: Long,
	prevLogTerm: Long,
	leaderCommit: Long,
	entries: List[LogEntry]
)