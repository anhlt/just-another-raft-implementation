package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/**
  * Response to an append entries request
  * @param nodeId the address of the node that is responding
  * @param currentTerm the term of the request
  * @param ack the index of the last log entry
  * @param success true if the append was successful, false otherwise
  */
case class AppendEntriesResponse(
	nodeId: NodeAddress,
	currentTerm: Long,
	ack: Long,
	success: Boolean
)
