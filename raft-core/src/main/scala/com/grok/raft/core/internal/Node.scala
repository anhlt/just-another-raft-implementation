package com.grok.raft.core.internal

import com.grok.raft.core.storage.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*

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
    *   - Compare the follower's log length with the prevSentLogIndex provided in the request.
    *   - If the follower's log is sufficiently long, ensure that the log entry at the prevSentLogIndex index matches
    *     the prevLogTerm in the request.
    *   - Reject the request if any inconsistencies, such as gaps or term mismatches, are detected.
    *
    * @param logRequest
    *   the log replication request from the leader.
    * @param logState
    *   the current state of the local log.
    * @param logEntryAtPrevSent
    *   the log entry at the specified prevSentLogIndex.
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

  /** This method is called when a log entry is replicated to the followers
    * No ops on Candidate and Follower nodes
    * @param configCluster
    * @return
    */
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

    if (currentLeader.isDefined) {
      (state, ResetLeaderAnnouncer :: actions)
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
      candidateLogIndex,
      candidateLastLogTerm
    ) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLogIndex >= logState.lastLogIndex)

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
      prevSentLogIndex,
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
            logState.lastLogIndex,
            false
          ),
          List.empty[Action]
        )
      )
    } else {
      val nextState =
        this.copy(currentTerm = leaderTerm, currentLeader = Some(leaderId))


      // When stepping down to follower due to a higher term leader (leaderId),
      // we must inform the system about the new leader and reset any previous leadership state.
      // This prevents the system from holding stale leadership information which could cause conflicts or stale reads.
      // Setting `resetPrevious = true` signals that any cached or remembered leader info should be cleared,
      // ensuring listeners or components observing leadership see the fresh and correct leader.
      // Also, persisting state via StoreState guarantees durability of the updated term and leadership changes,
      // which is critical before announcing a new leader to prevent inconsistencies after crashes.
      // This is aligned with Raft Paper §5.2 (Election Safety) ensuring at most one leader per term,
      // and distributed systems principles around safe state propagation during role changes.
      val announceLeaderAction =
        if (!currentLeader.contains(leaderId))
          // announce the new leader if it is different from the current leader
          List(
            AnnounceLeader(
              leaderId = leaderId,
               resetPrevious = true // reset when leader changes
            )
          )
        else List.empty[Action]

      val actions = StoreState :: announceLeaderAction

      if (
        logState.logLength > prevSentLogIndex &&
        logEntryAtPrevSent
          .map(_.term == prevLastLogTerm)
          .getOrElse(prevSentLogIndex == -1)
      ) {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
               prevSentLogIndex + logRequest.entries.length,
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
              -1, // TODO: verify
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

  def leader(): Option[NodeAddress] = currentLeader

  def toPersistedState: PersistedState = PersistedState(term = currentTerm , votedFor = votedFor)

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
      VoteRequest(address, newTerm, logState.lastLogIndex, newLastLogTerm)

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
      candidateLogIndex,
      candidateLastLogTerm
    ) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLogIndex >= logState.lastLogIndex)

    val termOk =
      candidateTerm > currentTerm || (candidateTerm == currentTerm && votedFor
        .contains(proposedLeaderAddress))

    (logOk && termOk) match
      case true =>
        (
          Follower(address, candidateTerm, Some(proposedLeaderAddress), votedFor=Some(proposedLeaderAddress)),
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
    val logIndex = logState.lastLogIndex

    if (term == currentTerm && voteGranted && newVoteReceived.size >= clusterConfiguration.quorumSize) {
      // construct the leader state
      val sentIndexMap = clusterConfiguration.members
        .filter(_ != address)
        .map(node => (node, logIndex)) // use last index (0-based)
        .toMap
      val ackedIndexMap = clusterConfiguration.members
        .filter(_ != address)
        .map(node => (node, -1L)) // -1 means no entries acknowledged yet
        .toMap
      val actions = clusterConfiguration.members.filter(_ != address).map(n => ReplicateLog(n, currentTerm, logIndex))


      (
        Leader(address, currentTerm, sentIndexMap, ackedIndexMap),
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
      prevSentLogIndex,
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
            logState.lastLogIndex,
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
        logState.logLength > prevSentLogIndex &&
        logEntryAtPrevSent
          .map(_.term == prevLastLogTerm)
          .getOrElse(prevSentLogIndex == -1)
      ) {
        (
          nextState,
          (
            LogRequestResponse(
              address,
              leaderTerm,
               prevSentLogIndex + logRequest.entries.length,
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
              -1, // TODO: verify
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

  def toPersistedState: PersistedState = PersistedState(term = currentTerm , votedFor = votedFor)

}

/** Represents the behavior and state of a Raft node when it is acting as the leader.
  *
  * A leader in Raft is responsible for:
  *   - Managing log replication to follower nodes,
  *   - Handling client commands and appending them to the replicated log,
  *   - Maintaining leadership authority via periodic heartbeats,
  *   - Tracking the highest log entries replicated and committed,
  *   - Responding to term changes and stepping down if a higher-term leader is detected.
  *
  * This implementation tracks two key maps:
  *   - `sentIndexMap`: The highest log index sent to each follower (equivalent to `nextIndex` in the Raft paper),
  *   - `ackIndexMap`: The highest log index acknowledged (matched) by each follower (equivalent to `matchIndex`).
  *
  * These mappings are used to determine when log entries are safely replicated on a majority and can be considered
  * committed (Raft Paper §5.3, §5.4).
  *
  * Leadership safety properties enforced here include:
  *   - Only one leader per term exists (Election Safety, Raft Paper §5.2),
  *   - Leaders never overwrite committed entries (Leader Completeness Property, Raft Paper §5.4.3),
  *   - Committed entries are durable and applied in order (State Machine Safety, Raft Paper §5.4).
  *
  * This class also handles stepping down to a follower on discovering a higher term from other nodes, guaranteeing
  * system consistency in the presence of partitions or term changes.
  *
  * @param address
  *   The unique network address of the leader node.
  * @param currentTerm
  *   The term number the leader is operating in.
  * @param sentIndexMap
  *   Tracks the highest log entry index sent to each follower (equivalent to `nextIndex`, Raft Paper Figure 2).
  * @param ackIndexMap
  *   Tracks the highest log entry index acknowledged by each follower (equivalent to `matchIndex`).
  * @param currentLeader
  *   (Optionally) the current leader address (usually self).
  *
  * @see
  *   Raft Paper, Section 5.3 (Log Replication)
  * @see
  *   Raft Paper, Section 5.4 (Safety)
  * @see
  *   Raft Paper, Section 5.2 (Leader Election)
  * @see
  *   Distributed Systems Lecture, Section 6.2 (Leader Responsibilities and Log Replication Safety)
  */
case class Leader(
    val address: NodeAddress,
    val currentTerm: Long,
    val sentIndexMap: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long],
    val ackIndexMap: Map[NodeAddress, Long] = Map.empty[NodeAddress, Long],
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

    val VoteRequest(candidateAddress, candidateTerm, candidateLogIndex, candidateLastLogTerm) = voteRequest

    val lastLogTerm = logState.lastLogTerm.getOrElse(currentTerm)

    // Check if candidate’s log is at least as up-to-date as leader’s log (§5.4)
    val logOk =
      (candidateLastLogTerm > lastLogTerm) ||
        (candidateLastLogTerm == lastLogTerm && candidateLogIndex >= logState.lastLogIndex)

    val termOk = candidateTerm > currentTerm

    if (logOk && termOk) {
      // New legitimate leader detected: step down to follower and reset leader state
      val newState = Follower(address, candidateTerm, Some(candidateAddress))
      val response = VoteResponse(address, candidateTerm, true)
      val actions  = List(StoreState, ResetLeaderAnnouncer) // persist and reset leadership
      (newState, (response, actions))
    } else {
      // Reject vote but update replication progress for candidate to help log sync
      val candidateLastIndex = candidateLogIndex // convert length to index
      val updatedSentIndexMap = this.sentIndexMap + (candidateAddress -> candidateLastIndex)
      val updatedAckIndexMap  = this.ackIndexMap + (candidateAddress  -> candidateLastIndex)
      val newState             = this.copy(sentIndexMap = updatedSentIndexMap, ackIndexMap = updatedAckIndexMap)
      val response             = VoteResponse(address, currentTerm, false)
      // Trigger replication to help candidate catch up
      val actions = List(ReplicateLog(candidateAddress, currentTerm, candidateLastIndex))
      (newState, (response, actions))
    }
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

  /** Handles incoming AppendEntries (log replication) RPCs from another leader. If a higher term is detected, leader
    * steps down to follower to maintain Election Safety. Otherwise, replies with success or failure according to log
    * consistency checks.
    */
  def onLogRequest(
      logRequest: LogRequest,
      logState: LogState,
      logEntryAtPrevSent: Option[LogEntry],
      clusterConfiguration: ClusterConfiguration
  ): (Node, (LogRequestResponse, List[Action])) = {

    if (logRequest.term < currentTerm) {
      // RPC term is stale: reject replication request
      val response = LogRequestResponse(address, currentTerm, logState.lastLogIndex, success = false)
      (this, (response, List.empty))
    } else {
      // Higher or equal term from another leader or candidate: step down to follower
      // When stepping down to follower due to a higher term leader (leaderId),
      // we must inform the system about the new leader and reset any previous leadership state.
      // This prevents the system from holding stale leadership information which could cause conflicts or stale reads.
      //
      // Setting `resetPrevious = true` signals that any cached or remembered leader info should be cleared,
      // ensuring listeners or components observing leadership see the fresh and correct leader.
      //
      // Also, persisting state via StoreState guarantees durability of the updated term and leadership changes,
      // which is critical before announcing a new leader to prevent inconsistencies after crashes.
      //
      // This is aligned with Raft Paper §5.2 (Election Safety) ensuring at most one leader per term,
      // and distributed systems principles around safe state propagation during role changes.
      val nextState = Follower(address, logRequest.term, currentLeader = Some(logRequest.leaderId))
      val actions   = List(StoreState, AnnounceLeader(logRequest.leaderId, resetPrevious = true))

      val logLengthCheck = logState.logLength > logRequest.prevSentLogIndex
      val termMatch =
        logEntryAtPrevSent.map(_.term == logRequest.prevLastLogTerm).getOrElse(logRequest.prevSentLogIndex == -1)

      if (logLengthCheck && termMatch) {
        // Log is consistent; accept entries
        val response = LogRequestResponse(
          address,
          logRequest.term,
          logRequest.prevSentLogIndex + logRequest.entries.length,
          success = true
        )
        (nextState, (response, actions))
      } else {
        // Log inconsistency: reject entries
        val response = LogRequestResponse(address, logRequest.term, -1, success = false)
        (nextState, (response, actions))
      }
    }
  }

  /** Handles the response to a log replication request (AppendEntries RPC) from a follower.
    *
    * method has critical responsibilities for maintaining the leader's view of followers' logs, updating replication
    * indexes, retrying replication on failure, and committing entries once replicated on a majority, thereby preserving
    * Raft's safety and liveness properties.
    *
    * The method performs the following:
    *   1. Detects if the follower reports a higher term, which triggers the leader to step down, ensuring Election
    *      Safety (§5.2). 2. On success:
    *      - Updates the sent and acknowledged log indices (equivalent to `nextIndex` and `matchIndex` in the Raft paper
    *        Figure 2).
    *      - Triggers commit of log entries that have been replicated on a majority, upholding Leader Completeness
    *        (§5.4.3).
    *      3. On failure (usually due to log inconsistencies):
    *      - Decreases the sent index for the follower (`nextIndex`) to retry replication with earlier log entries,
    *        implementing Raft’s backtracking mechanism to resolve conflicts (§5.3).
    *      - Initiates a new replication action with the updated prefixLength.
    *
    * @param logState
    *   Current local log state of the leader including the last applied index.
    * @param config
    *   Full cluster membership information, used to determine quorum.
    * @param msg
    *   LogRequestResponse from a follower, indicating success or failure of a replication attempt.
    *
    * @return
    *   A tuple of updated `Node` (leader with adjusted replication state or follower after stepping down) and a list of
    *   protocol `Action`s that include committing logs, retrying replication, or stepping down.
    *
    * @see
    *   Raft Paper, Figure 2 (nextIndex, matchIndex)
    * @see
    *   Raft Paper, Section 5.2 (Election Safety)
    * @see
    *   Raft Paper, Section 5.3 (Log Replication Backtracking)
    * @see
    *   Raft Paper, Section 5.4.3 (Leader Completeness Property)
    */
  def onLogRequestResponse(
      logState: LogState,
      config: ClusterConfiguration,
      msg: LogRequestResponse
  ): (Node, List[Action]) = {

    val LogRequestResponse(fromNodeId, messageTerm, ackLogIndex, success) = msg

    if (messageTerm > currentTerm) {
      // Step down if follower reports higher term to maintain Election Safety (§5.2)
      val newState = Follower(address, messageTerm, None, None)
      // Persist new state and reset leader announcer to start fresh elections if needed
      (newState, List(StoreState, ResetLeaderAnnouncer))
    } else {
      if (success) {
        // Replication succeeded: update sent and ack maps with follower's ack index
        val newSentIndexMap = sentIndexMap + (fromNodeId -> ackLogIndex)
        val newAckIndexMap  = ackIndexMap + (fromNodeId  -> ackLogIndex)

        // Combine with self's applied log index to determine commit indices
        val combinedAckMap = newAckIndexMap + (address -> logState.appliedLogIndex)

        // Trigger commit action: entries replicated on majority can now be applied (§5.4)
        val commitAction = CommitLogs(combinedAckMap)
        (this.copy(sentIndexMap = newSentIndexMap, ackIndexMap = newAckIndexMap), List(commitAction))

      } else {
        // Replication failed: decrease sentIndex (nextIndex) for follower and retry (§5.3)
        val updatedSentIndex = sentIndexMap.get(fromNodeId) match {
          case Some(sentIndex) if sentIndex > -1 => sentIndex - 1
          case _                                 => -1L
        }
        val newSentIndexMap = sentIndexMap + (fromNodeId -> updatedSentIndex)

        // Retry log replication with lower prefixIndex for conflict resolution
        (
          this.copy(sentIndexMap = newSentIndexMap),
          List(StoreState, ReplicateLog(fromNodeId, currentTerm, updatedSentIndex))
        )
      }
    }
  }

  def onReplicateLog(configCluster: ClusterConfiguration): List[Action] = {
    configCluster.members.filter(_ != address).map { node =>
      ReplicateLog(node, currentTerm, sentIndexMap.getOrElse(node, -1L))
    }
  }

  def onSnapshotInstalled(
      logState: LogState,
      clusterConfiguration: ClusterConfiguration
  ): (Node, LogRequestResponse) = ???

  def leader(): Option[NodeAddress] = Some(address)

  def toPersistedState: PersistedState = PersistedState(term = currentTerm , votedFor = Some(address))

}
