package com.grok.raft.core.internal

import com.grok.raft.core.protocol.Action
import com.grok.raft.core.protocol.VoteRequest
import com.grok.raft.core.protocol.VoteResponse
import com.grok.raft.core.protocol.AppendEntriesResponse
import com.grok.raft.core.storage.PersistedState
import com.grok.raft.core.protocol.RequestForVote
import com.grok.raft.core.protocol.StoreState

/** The Node state represents the State Machine in Raft Protocol The possible
  * states are:
  *   - Follower
  *   - Candidate
  *   - Leader
  */
sealed trait Node {
  val address: NodeAddress
  val currentTerm: Long
  val votedFor: Option[NodeAddress]
  val log: List[LogEntry]
  val commitLength: Long

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
    val votedFor: Option[NodeAddress],
    val log: List[LogEntry],
    val commitLength: Long
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
    val log: List[LogEntry],
    val commitLength: Long,
    val lastLogTerm: Long
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = {

  	val newTerm = currentTerm + 1
  	val newLastLogTerm = logState.lastLogTerm.getOrElse(lastLogTerm) // Potential bug. Need to check later
  	val voteRequest = VoteRequest(address, newTerm, logState.lastLogIndex, newLastLogTerm)

  	val actions = clusterConfiguration.members.filterNot(_ == address).map(peerAddress => RequestForVote(peerAddress, voteRequest))

  	(
  		this.copy(currentTerm = newTerm, lastLogTerm = newLastLogTerm, votedFor = Some(address), voteReceived = Set(address)),
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

case class Leader(
    val address: NodeAddress,
    val currentTerm: Long,
    val votedFor: Option[NodeAddress],
    val log: List[LogEntry],
    val commitLength: Long
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
