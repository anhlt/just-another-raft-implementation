package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/** Request for a vote
  * @param proposedLeaderAddress
  *   the address of the node that is requesting the vote
  * @param term
  *   the term of the vote request, The value of the term is the currentTerm of the candidate requesting the vote plus
  *   one
  * @param candidateLogIndex
  *   the index of the last log entry
  * @param lastLogTerm
  *   the term of the last log entry
  */
case class VoteRequest(proposedLeaderAddress: NodeAddress, candidateTerm: Long, candidateLogIndex: Long, candidateLastLogTerm: Long)
