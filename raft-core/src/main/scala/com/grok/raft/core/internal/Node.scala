package com.grok.raft.core.internal

import com.grok.raft.core.storage.PersistedState
import com.grok.raft.core.protocol._

/** The Node state represents the State Machine in Raft Protocol The possible states are:
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

  /** Called when a VoteResponse message is received from a peer node.
    *
    * This method processes the VoteResponse by:
    *   - Validating the response to ensure it comes from a legitimate Raft peer.
    *   - Checking term numbers and candidate identifiers to verify consistency with the current node state.
    *   - Updating the current vote tally if the response supports the candidate role.
    *   - Determining if a quorum has been reached to trigger a leader election victory.
    *   - Resetting election timeouts and adjusting internal state as necessary.
    *
    * Proper handling of a VoteResponse is critical to maintaining the correctness and safety of the Raft consensus
    * algorithm, ensuring that leader elections are conducted accurately and that the system remains robust in the
    * presence of network partitions or delayed messages.
    *
    * @param voteResponse
    *   the VoteResponse message received from a peer node
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onVoteResponse(
      voteResponse: VoteResponse,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action])

  /** Processes an incoming LogRequest (AppendEntries) message.
    *
    * This method performs the following operations:
    *
    *   1. Term Update:
    *      - If the term in the LogRequest exceeds the current term, update the node's term.
    *      - Clear any previously recorded vote by setting votedFor to None.
    *
    * 2. Log Consistency Check:
    *   - Verify the consistency of the local log with the leader's log.
    *   - Compare the follower’s log length with the prevSentLogLength provided in the request.
    *   - If the follower's log is sufficiently long, ensure that the log entry at the prevSentLogLength index matches
    *     the prevLogTerm in the request.
    *   - Reject the request if any inconsistencies, such as gaps or term mismatches, are detected.
    *
    * @param logRequest
    *   the log replication request from the leader.
    * @param logState
    *   the current state of the local log.
    * @param logEntryAtPrevSent
    *   the log entry at the specified prevSentLogLength.
    * @param clusterConfiguration
    *   the current cluster configuration details.
    *
    * @return
    *   a tuple consisting of the updated Node state and a tuple of LogRequestResponse along with any generated actions.
    */
  def onLogRequest(
      logRequest: LogRequest,
      logState: LogState,
      logEntryAtPrevSent: Option[LogEntry],
      clusterConfiguration: ClusterConfiguration
  ): (Node, (LogRequestResponse, List[Action]))

  def onLogRequestResponse(
      logState: LogState,
      config: ClusterConfiguration,
      msg: LogRequestResponse
  ): (Node, List[Action])

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action]

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, LogRequestResponse)

  def leader(): Option[NodeAddress]

  def toPersistedState: PersistedState

}

case class Follower(
    val address: NodeAddress,
    val currentTerm: Long,
    val currentLeader: Option[NodeAddress] = None,
    val votedFor: Option[NodeAddress] = None
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = {

    val (state, actions) = Candidate(address, currentTerm).onTimer(
      logState,
      clusterConfiguration
    )

    if (currentLeader.isDefined){
      (state,  ResetLeaderAnnouncer :: actions)
    } else {
      (state, actions)
    }
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
    val VoteRequest(
      proposedLeaderAddress,
      candidateTerm,
      candidateLogLength,
      candidateLastLogTerm
    ) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLogLength >= logState.logLength)

    val termOk =
      candidateTerm > currentTerm || (candidateTerm == currentTerm && votedFor
        .contains(proposedLeaderAddress))

    (logOk && termOk) match
      case true =>
        (
          this.copy(currentTerm = candidateTerm, votedFor = Some(proposedLeaderAddress)),
          (
            VoteResponse(address, candidateTerm, logOk && termOk),
            List(StoreState)
          )
        )
      case false =>
        (
          this,
          (
            VoteResponse(address, currentTerm, logOk && termOk),
            List.empty[Action]
          )
        )
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
  ): (Node, List[Action]) = (this, List.empty[Action])

  /** This method is called when a LogRequest is received
    * @param logRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onLogRequest(
      logRequest: LogRequest,
      logState: LogState,
      logEntryAtPrevSent: Option[LogEntry],
      clusterConfiguration: ClusterConfiguration
  ): (Node, (LogRequestResponse, List[Action])) = {

    val LogRequest(
      leaderId,
      leaderTerm,
      prevSentLogLength,
      prevLastLogTerm,
      _,
      _
    ) = logRequest

    if (currentTerm > leaderTerm) {
      (
        this,
        (
          LogRequestResponse(
            address,
            currentTerm,
            logState.logLength,
            false
          ),
          List.empty[Action]
        )
      )
    } else {
      val nextState =
        this.copy(currentTerm = leaderTerm, currentLeader = Some(leaderId))

      val announceLeaderAction =
        if (currentLeader.contains(leaderId))
          List(
            AnnounceLeader(
              leaderId = leaderId,
              resetPrevious = currentLeader.isEmpty
            )
          )
        else List.empty[Action]

      val actions = StoreState :: announceLeaderAction

      if (
        logState.logLength >= prevSentLogLength &&
        logEntryAtPrevSent
          .map(_.term == prevLastLogTerm)
          .getOrElse(prevSentLogLength == 0)
      ) {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
              prevSentLogLength + logRequest.entries.length,
              true
            ),
            actions
          )
        )

      } else {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
              0, // TODO: verify
              false
            ),
            actions
          )
        )
      }
    }

  }

  def onLogRequestResponse(
      logState: LogState,
      config: ClusterConfiguration,
      msg: LogRequestResponse
  ): (Node, List[Action]) = (this, List.empty[Action])

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = List.empty[Action]

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, LogRequestResponse) = ???

  def leader(): Option[NodeAddress] = ???

  def toPersistedState: PersistedState = ???

}

case class Candidate(
    val address: NodeAddress,
    val currentTerm: Long,
    val votedFor: Option[NodeAddress] = None,
    val voteReceived: Set[NodeAddress] = Set.empty[NodeAddress]
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = {

    val newTerm = currentTerm + 1
    val newLastLogTerm =
      logState.lastLogTerm.getOrElse(0L) // Potential bug. Need to check later
    val voteRequest =
      VoteRequest(address, newTerm, logState.logLength, newLastLogTerm)

    val actions = clusterConfiguration.members
      .filterNot(_ == address)
      .map(peerAddress => RequestForVote(peerAddress, voteRequest))

    (
      this.copy(
        currentTerm = newTerm,
        votedFor = Some(address),
        voteReceived = Set(address)
      ),
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
    val VoteRequest(
      proposedLeaderAddress,
      candidateTerm,
      candidateLogLength,
      candidateLastLogTerm
    ) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLogLength >= logState.logLength)

    val termOk =
      candidateTerm > currentTerm || (candidateTerm == currentTerm && votedFor
        .contains(proposedLeaderAddress))

    (logOk && termOk) match
      case true =>
        (
          Follower(address, candidateTerm, Some(proposedLeaderAddress)),
          (
            VoteResponse(address, candidateTerm, logOk && termOk),
            List(StoreState)
          )
        )
      case false =>
        (
          this,
          (
            VoteResponse(address, currentTerm, logOk && termOk),
            List.empty[Action]
          )
        )
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

    val newVoteReceived =
      if (voteGranted) voteReceived + responseAddress else voteReceived
    val logLength = logState.logLength

    if (term == currentTerm && voteGranted && newVoteReceived.size >= clusterConfiguration.quorumSize) {
      // construct the leader state
      val sentLenghtMap = clusterConfiguration.members
        .filter(_ != address)
        .map(node => (node, logLength)) // we think we lenght of log
        .toMap
      val ackedLengthMap = clusterConfiguration.members
        .filter(_ != address)
        .map(node => (node, 0L))
        .toMap
      val actions = clusterConfiguration.members.map(n => ReplicateLog(n, currentTerm, logLength))

      (
        Leader(address, currentTerm, sentLenghtMap, ackedLengthMap),
        StoreState :: AnnounceLeader(address) :: actions
      )

    } else {
      (this.copy(voteReceived = newVoteReceived), List.empty[Action])
    }
  }

  /** This method is called when a LogRequest is received Note that the candidate will become a follower if the term of
    * the request is greater than its current term.
    *
    * @param logRequest
    * @param logState
    * @param logEntryAtPrevSent
    * @param clusterConfiguration
    * @return
    */
  def onLogRequest(
      logRequest: LogRequest,
      logState: LogState,
      logEntryAtPrevSent: Option[LogEntry],
      clusterConfiguration: ClusterConfiguration
  ): (Node, (LogRequestResponse, List[Action])) = {

    val LogRequest(
      leaderId,
      leaderTerm,
      prevSentLogLength,
      prevLastLogTerm,
      _,
      _
    ) = logRequest

    if (leaderTerm < currentTerm) {
      (
        this,
        (
          LogRequestResponse(
            address,
            currentTerm,
            logState.logLength,
            false
          ),
          List.empty[Action]
        )
      )
    } else {
      val nextState = Follower(
        address,
        logRequest.term,
        currentLeader = Some(leaderId)
      )
      val actions =
        List(StoreState, AnnounceLeader(leaderId))

      // check if current log on the candidate is long enough
      // and if it long enough, check if the term of at logEntry At PrevLogIndex is the same as the prevLogTerm
      if (
        logState.logLength >= prevSentLogLength &&
        logEntryAtPrevSent
          .map(_.term == prevLastLogTerm)
          .getOrElse(prevSentLogLength == 0)
      ) {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
              prevSentLogLength + logRequest.entries.length,
              true
            ),
            actions
          )
        )

      } else {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
              0, // TODO: verify
              false
            ),
            actions
          )
        )
      }
    }

  }

  def onLogRequestResponse(
      logState: LogState,
      config: ClusterConfiguration,
      msg: LogRequestResponse
  ): (Node, List[Action]) = (this, List.empty[Action])

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] =
    List.empty[Action]

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, LogRequestResponse) = ???

  def leader(): Option[NodeAddress] = None

  def toPersistedState: PersistedState = ???

}

/*
 * The Leader state defines the behavior of a node serving as the leader in the Raft consensus protocol.
 *
 * @param address       The unique network address of the node.
 * @param currentTerm   The term number that the node is currently operating in.
 * @param sentIndex     A mapping from each peer node's address to the highest log index that has been transmitted to that peer.
 * @param ackedIndex    A mapping from each peer node's address to the highest log index that has been confirmed as received by that peer.
 */

case class Leader(
    val address: NodeAddress,
    val currentTerm: Long,
    val sentLenghtMap: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long],
    val ackLengthMap: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long],
    val currentLeader: Option[NodeAddress] = None
) extends Node {

  def onTimer(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, List[Action]) = (this, List.empty)

  /** Processes an incoming VoteRequest.
    *
    * This method evaluates two primary conditions:
    *   - logOK: Ensures that the candidate’s log is at least as up-to-date as the current node's log.
    *   - termOK: Validates that the candidate's term is acceptable relative to the current node's term.
    *
    * Behavior:
    *   - If both logOK and termOK conditions are met, the node transitions into a follower state, acknowledging the
    *     candidate’s successful request.
    *   - If either condition fails, the node maintains its leader state. However, the candidate is instructed to
    *     transition to a follower state. Additionally, the node initiates log replication towards the candidate. This
    *     replication (ReplicateLog) ensures that the candidate's log becomes consistent with the leader's log, thereby
    *     preserving the overall integrity and consistency of the distributed log across the cluster.
    * @see
    *   [[com.grok.raft.core.protocol.ReplicateLog]]
    *
    * @param voteRequest
    *   the incoming request for a vote.
    * @param logState
    *   the current state of the log.
    * @param clusterConfiguration
    *   the cluster's current configuration.
    * @return
    *   a tuple containing:
    *   - the updated node state, and
    *   - a list of actions that need to be executed (which may include instructing the candidate to switch to a
    *     follower state and initiating the ReplicateLog process to synchronize the candidate's log).
    */
  def onVoteRequest(
      voteRequest: VoteRequest,
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, (VoteResponse, List[Action])) = {

    val VoteRequest(
      candidateAddress,
      candidateTerm,
      candidateLogLength,
      candidateLastLogTerm
    ) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(currentTerm)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLogLength >= logState.logLength)

    val termOk = candidateTerm > currentTerm

    (logOk && termOk) match
      case true =>
        (
          Follower(address, candidateTerm, Some(candidateAddress)),
          (
            VoteResponse(address, candidateTerm, logOk && termOk),
            List(StoreState, ResetLeaderAnnouncer)
          )
        )
      case false =>
        (
          this.copy(
            sentLenghtMap = this.sentLenghtMap + (candidateAddress -> candidateLogLength),
            ackLengthMap = this.ackLengthMap + (candidateAddress   -> candidateLogLength)
          ),
          (
            VoteResponse(address, currentTerm, logOk && termOk),
            List(ReplicateLog(candidateAddress, currentTerm, candidateLogLength))
          )
        )

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
  ): (Node, List[Action]) = ???

  /** This method is called when a LogRequest is received
    * @param logRequest
    * @param logState
    * @param clusterConfiguration
    * @return
    */
  def onLogRequest(
      logRequest: LogRequest,
      logState: LogState,
      logEntryAtPrevSent: Option[LogEntry],
      clusterConfiguration: ClusterConfiguration
  ): (Node, (LogRequestResponse, List[Action])) = {

    if (logRequest.term < currentTerm) {
      (
        this,
        (
          LogRequestResponse(
            address,
            currentTerm,
            logState.logLength,
            false
          ),
          List.empty[Action]
        )
      )
    } else {
      val nextState = Follower(
        address,
        logRequest.term,
        currentLeader = Some(logRequest.leaderId)
      )
      val actions =
        List(StoreState, AnnounceLeader(logRequest.leaderId, resetPrevious = true))

      // check if current log on the candidate is long enough by comparing it with prefix Log Length info from leader
      // and if it long enough, check if the term of at logEntry At PrevLogIndex is the same as the prevLogTerm
      if (
        logState.logLength >= logRequest.prevSentLogLength &&
        logEntryAtPrevSent
          .map(_.term == logRequest.prevLastLogTerm)
          .getOrElse(logRequest.prevSentLogLength == 0)
      ) {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              logRequest.term,
              logRequest.prevSentLogLength + logRequest.entries.length,
              true
            ),
            actions
          )
        )

      } else {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              logRequest.term,
              0,
              false
            ),
            actions
          )
        )
      }
    }

  }

  def onLogRequestResponse(
      logState: LogState,
      config: ClusterConfiguration,
      msg: LogRequestResponse
  ): (Node, List[Action]) = {

    val LogRequestResponse(fromNodeId, messageTerm, ackLogLength, success) = msg

    if (messageTerm > currentTerm) {
      (Follower(this.address, messageTerm, None, None), List(StoreState, ResetLeaderAnnouncer))
    } else {
      if (success) {
        (
          this.copy(
            sentLenghtMap = sentLenghtMap + (fromNodeId -> ackLogLength),
            ackLengthMap = ackLengthMap + (fromNodeId   -> ackLogLength)
          ),
          List(CommitLogs(ackLengthMap + (fromNodeId -> ackLogLength) + (address -> logState.appliedLogLength)))
        )
      } else {
        val updatedSentLenght = sentLenghtMap.get(fromNodeId) match {
          case Some(sentLength) if sentLength >= 1 => sentLength - 1
          case Some(sentLength)                    => 0
          case None                                => 0
        }

        (
          this.copy(sentLenghtMap = sentLenghtMap + (fromNodeId -> updatedSentLenght)),
          List(StoreState, ReplicateLog(fromNodeId, currentTerm, updatedSentLenght))
        )
      }

    }

  }

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = {
    configCluster.members.filter(_ != address).map { node =>
      ReplicateLog(node, currentTerm, sentLenghtMap.getOrElse(node, 0L))
    }
  }

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, LogRequestResponse) = ???

  def leader(): Option[NodeAddress] = Some(address)

  def toPersistedState: PersistedState = ???

}
