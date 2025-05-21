package com.grok.raft.core.protocol

import com.grok.raft.core.internal.NodeAddress

/**
 * Response to an append entries request.
 *
 * When the append operation is successful, the leader updates the ackLengthMap with the provided
 * ackLogLenght value. The leader is then able to commit the message once the ackLengthMap reflects
 * acknowledgments from a majority of the cluster.
 *
 * For example, consider a cluster with five nodes having the following ackLogLenght values:
 *   - Node 1: 3
 *   - Node 2: 5
 *   - Node 3: 4
 *   - Node 4: 4
 *   - Node 5: 6
 *
 * Given a quorum size of 3, the message can be committed if at least three nodes have an ackLogLenght
 * of 4 or higher.
 *
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
    val nodeId: NodeAddress,
    val currentTerm: Long,
    val ackLogLenght: Long,
    val success: Boolean
)
