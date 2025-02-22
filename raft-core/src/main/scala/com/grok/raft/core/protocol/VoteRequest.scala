package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/**
  * Request for a vote
  * @param nodeAddress the address of the node that is requesting the vote
  * @param term the term of the vote request, The value of the term is the currentTerm of the candidate requesting the vote plus one
  * @param lastLogIndex the index of the last log entry
  * @param lastLogTerm the term of the last log entry
  */
case class VoteRequest(nodeAddress: NodeAddress ,term: Long, lastLogIndex: Long, lastLogTerm: Long)