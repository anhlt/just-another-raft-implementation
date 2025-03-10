package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/** Response to a vote request
  * @param nodeAddress
  *   the address of the node that is responding
  * @param term
  *   the term of the vote request
  * @param voteGranted
  *   true if the vote was granted, false otherwise
  */
case class VoteResponse(nodeAddress: NodeAddress, term: Long, voteGranted: Boolean)
