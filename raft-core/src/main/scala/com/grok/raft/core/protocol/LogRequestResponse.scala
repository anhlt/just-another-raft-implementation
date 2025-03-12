package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/** Response to an append entries request
  * @param nodeId
  *   the address of the node that is responding
  * @param currentTerm
  *   the term of the request
  * @param ackLogLenght
  *   the lenght of the log have been acked by receiver
  * @param success
  *   true if the append was successful, false otherwise
  */
case class LogRequestResponse(
    nodeId: NodeAddress,
    currentTerm: Long,
    ackLogLenght: Long,
    success: Boolean
)
