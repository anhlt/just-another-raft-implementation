package com.grok.raft.core.internal

import com.grok.raft.core.protocol.Action
import com.grok.raft.core.protocol.VoteRequest
import com.grok.raft.core.protocol.VoteResponse
import com.grok.raft.core.protocol.AppendEntriesResponse
import com.grok.raft.core.storage.PersistedState
import com.grok.raft.core.protocol.RequestForVote
import com.grok.raft.core.protocol.StoreState
import com.grok.raft.core.protocol.ReplicateLog
import com.grok.raft.core.protocol.AnnounceLeader

/** The Node state represents the State Machine in Raft Protocol The possible
  * states are:
  *   - Follower
  *   - Candidate
  *   - Leader
  */
sealed trait Node {
  val address: NodeAddress
  val currentTerm: Long
  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action])

  /** This method is called when a VoteRequest is received
    * @param voteRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteRequest(
      voteRequest: VoteRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (VoteResponse, List[Action]))

  /** This method is called when a VoteResponse is received
    * @param voteResponse
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteResponse(
      voteResponse: VoteResponse,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action])

  /** This method is called when a AppendEntriesRequest is received
    * @param appendEntriesRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onAppendEntriesRequest(
      appendEntriesRequest: AppendEntriesRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (AppendEntriesResponse, List[Action]))

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action]

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, AppendEntriesResponse)

  def leader(): Option[NodeAddress]

  def toPersistedState: PersistedState

}
case class Follower(
    val address: NodeAddress,
    val currentTerm: Long,
    val votedFor: Option[NodeAddress] = None,
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = ???

  /** This method is called when a VoteRequest is received
    * @param voteRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteRequest(
      voteRequest: VoteRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (VoteResponse, List[Action])) = ???

  /** This method is called when a VoteResponse is received
    * @param voteResponse
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteResponse(
      voteResponse: VoteResponse,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = ???

  /** This method is called when a AppendEntriesRequest is received
    * @param appendEntriesRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onAppendEntriesRequest(
      appendEntriesRequest: AppendEntriesRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (AppendEntriesResponse, List[Action])) = ???

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = ???

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, AppendEntriesResponse) = ???

  def leader(): Option[NodeAddress] = ???

  def toPersistedState: PersistedState = ???

}

case class Candidate(
    val address: NodeAddress,
    val currentTerm: Long,
    val votedFor: Option[NodeAddress],
    val voteReceived: Set[NodeAddress],
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = {

    val newTerm = currentTerm + 1
    val newLastLogTerm = logState.lastLogTerm.getOrElse(0L) // Potential bug. Need to check later
    val voteRequest = VoteRequest(address, newTerm, logState.lastLogIndex, newLastLogTerm)

    val actions = clusterConfiguration.members.filterNot(_ == address).map(peerAddress => RequestForVote(peerAddress, voteRequest))

    (
      this.copy(currentTerm = newTerm, votedFor = Some(address), voteReceived = Set(address)),
      StoreState :: actions
    )
  }

  /** This method is called when a VoteRequest is received
    * @param voteRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteRequest(
      voteRequest: VoteRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (VoteResponse, List[Action])) = {
    val VoteRequest(proposedLeaderAddress ,candidateTerm, candidateLastLogIndex, candidateLastLogTerm) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOk = (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= logState.lastLogIndex)

    val termOk = candidateTerm > currentTerm || (candidateTerm == currentTerm && votedFor.contains(proposedLeaderAddress))

    (logOk && termOk) match
      case true => (Follower(address, candidateTerm, Some(proposedLeaderAddress)), (VoteResponse(address, candidateTerm, logOk && termOk), List(StoreState)))
      case false => (this, (VoteResponse(address, currentTerm, logOk && termOk), List.empty[Action]))
  }

  /** This method is called when a VoteResponse is received
    * @param voteResponse
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteResponse(
      voteResponse: VoteResponse,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = {

    val VoteResponse(responseAddress, term, voteGranted) = voteResponse

    val newVoteReceived = if(voteGranted) voteReceived + responseAddress else voteReceived
    val logLength = logState.lastLogIndex + 1

    if (term == currentTerm && voteGranted && newVoteReceived.size >= clusterConfiguration.quorumSize) {
      // construct the leader state
      val sentLenghtMap = clusterConfiguration.members.filter(_ != address).map(node => (node, logLength)).toMap
      val ackedLengthMap = clusterConfiguration.members.filter(_ != address).map(node => (node, 0L)).toMap
      val actions = clusterConfiguration.members.map(n => ReplicateLog(n, currentTerm, logLength))

      (Leader(address, currentTerm, sentLenghtMap, ackedLengthMap), StoreState :: AnnounceLeader(address) :: actions)

    } else {
      (this.copy(voteReceived = newVoteReceived), List.empty[Action])
    }
  }

  /** This method is called when a AppendEntriesRequest is received
    * @param appendEntriesRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onAppendEntriesRequest(
      appendEntriesRequest: AppendEntriesRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (AppendEntriesResponse, List[Action])) = ???

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = ???

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, AppendEntriesResponse) = ???

  def leader(): Option[NodeAddress] = ???

  def toPersistedState: PersistedState = ???

}


/*  
  * The Leader state represents the Leader Node in Raft Protocol
  * @param address the address of the node
  * @param currentTerm the current term of the node
  * @param sentLenght the lenght of the log entries counted already sent to each node
  * @param ackedLength the lenght of the log entries counted from the beginning acked by each node
  */

case class Leader(
    val address: NodeAddress,
    val currentTerm: Long,
    val sentLenght: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long],
    val ackedLength: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long]
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = ???

  /** This method is called when a VoteRequest is received
    * @param voteRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteRequest(
      voteRequest: VoteRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (VoteResponse, List[Action])) = ???

  /** This method is called when a VoteResponse is received
    * @param voteResponse
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteResponse(
      voteResponse: VoteResponse,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = ???

  /** This method is called when a AppendEntriesRequest is received
    * @param appendEntriesRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onAppendEntriesRequest(
      appendEntriesRequest: AppendEntriesRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (AppendEntriesResponse, List[Action])) = ???

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = ???

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, AppendEntriesResponse) = ???

  def leader(): Option[NodeAddress] = ???

  def toPersistedState: PersistedState = ???

}
