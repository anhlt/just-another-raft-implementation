package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/** Response to an append entries request.
  *
  * When the append operation is successful, the leader updates the ackIndexMap with the provided ackLogIndex value. The
  * leader is then able to commit the message once the ackIndexMap reflects acknowledgments from a majority of the
  * cluster.
  *
  * For example, consider a cluster with five nodes having the following ackLogIndex values:
  *   - Node 1: 2
  *   - Node 2: 4
  *   - Node 3: 3
  *   - Node 4: 3
  *   - Node 5: 5
  *
  * Given a quorum size of 3, the message can be committed if at least three nodes have an ackLogIndex of 3 or higher.
  *
  * @param nodeId
  *   the address of the node that is responding
  * @param currentTerm
  *   the term of the request
  * @param ackLogIndex
  *   the highest log index that has been acknowledged by the receiver (0-based index)
  * @param success
  *   true if the append was successful, false otherwise
  */
case class LogRequestResponse(
    val nodeId: NodeAddress,
    val currentTerm: Long,
    val ackLogIndex: Long,
    val success: Boolean
)
