package com.grok.raft.core.internal

import munit.FunSuite

import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*

/** Focused verification of Raft term (epoch) handling.
  *
  * Raft requires:
  *   - A node starting an election increments its term by exactly one (Raft Paper 5.2)
  *   - The term carried in the outgoing VoteRequest equals the newly incremented term
  *   - A node adopts a strictly larger term seen in any RPC and never adopts a smaller one
  *   - Terms are monotonically non-decreasing across every state transition
  *   - A winning candidate becomes leader in the SAME term it campaigned in (no extra bump)
  *   - The persisted state always reflects the current (post-increment) term
  */
class RaftTermIncrementSuite extends FunSuite {

  // --- Fixtures ---

  val addrA = NodeAddress("n1", 9090)
  val addrB = NodeAddress("n2", 9090)
  val addrC = NodeAddress("n3", 9090)

  val cluster3: ClusterConfiguration =
    ClusterConfiguration(
      currentNode = Follower(address = addrA, currentTerm = 0L),
      members = List(addrA, addrB, addrC)
    )

  val emptyLog: LogState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)

  def logAt(index: Long, term: Long): LogState =
    LogState(lastLogIndex = index, lastLogTerm = Some(term), appliedLogIndex = index - 1)

  /** Extracts the terms of every VoteRequest emitted in an action list. */
  def voteRequestTerms(actions: List[Action]): List[Long] =
    actions.collect { case RequestForVote(_, req) => req.candidateTerm }

  // =====================================================================
  // 1. Election start increments the term by EXACTLY one
  // =====================================================================

  test("Follower.onTimer increments term by exactly one") {
    List(0L, 1L, 7L, 41L, 1000L).foreach { startTerm =>
      val (next, _) = Follower(address = addrA, currentTerm = startTerm).onTimer(emptyLog, cluster3)
      assertEquals(next.currentTerm, startTerm + 1, s"Follower at term $startTerm must move to ${startTerm + 1}")
      assert(next.isInstanceOf[Candidate], "Follower must become Candidate on election timeout")
    }
  }

  test("Candidate.onTimer increments term by exactly one (retry election)") {
    List(1L, 3L, 12L, 99L).foreach { startTerm =>
      val (next, _) = Candidate(address = addrA, currentTerm = startTerm).onTimer(emptyLog, cluster3)
      assertEquals(next.currentTerm, startTerm + 1, s"Candidate at term $startTerm must retry at ${startTerm + 1}")
      assert(next.isInstanceOf[Candidate], "Candidate stays Candidate when retrying an election")
    }
  }

  test("Leader.onTimer does NOT change the term") {
    val leader    = Leader(address = addrA, currentTerm = 5L)
    val (next, _) = leader.onTimer(emptyLog, cluster3)
    assertEquals(next.currentTerm, 5L, "Leader must not bump its own term on timer")
    assert(next.isInstanceOf[Leader])
  }

  test("election increment votes for self and resets the vote set to self only") {
    val (next, _) = Follower(address = addrA, currentTerm = 4L, votedFor = Some(addrB)).onTimer(emptyLog, cluster3)
    val cand      = next.asInstanceOf[Candidate]
    assertEquals(cand.currentTerm, 5L)
    assertEquals(cand.votedFor, Some(addrA), "New term must be voted for self")
    assertEquals(cand.voteReceived, Set(addrA), "Vote set must be reset to only the self-vote for the new term")
  }

  // =====================================================================
  // 2. Repeated timeouts produce a strictly increasing term sequence
  // =====================================================================

  test("repeated election timeouts yield strictly increasing terms (no gaps, no repeats)") {
    val start: Node = Follower(address = addrA, currentTerm = 0L)
    val trajectory  = List.iterate(start, 6)(n => n.onTimer(emptyLog, cluster3)._1)
    val terms       = trajectory.map(_.currentTerm)

    assertEquals(terms, List(0L, 1L, 2L, 3L, 4L, 5L), "Each timeout must add exactly one term")
    // strict monotonicity, expressed independently of the exact values
    assert(
      terms.sliding(2).forall { case List(a, b) => b > a; case _ => true },
      s"Terms must strictly increase, got $terms"
    )
  }

  test("outgoing VoteRequest carries the NEW term, never the old one") {
    val (next, actions) = Candidate(address = addrA, currentTerm = 4L).onTimer(emptyLog, cluster3)
    val terms           = voteRequestTerms(actions)

    assertEquals(terms.size, 2, "One VoteRequest per peer in a 3-node cluster")
    terms.foreach(t => assertEquals(t, 5L, "VoteRequest must advertise the incremented term"))
    assertEquals(next.currentTerm, 5L, "Node term and advertised term must agree")
  }

  test("VoteRequest term matches node term across successive election rounds") {
    val rounds = List.iterate[Node](Follower(address = addrA, currentTerm = 0L), 4)(n =>
      n.onTimer(emptyLog, cluster3)._1
    )
    rounds.foreach { node =>
      val (next, actions) = node.onTimer(emptyLog, cluster3)
      voteRequestTerms(actions).foreach { advertised =>
        assertEquals(advertised, next.currentTerm, "Advertised term must equal the node's post-increment term")
        assertEquals(advertised, node.currentTerm + 1, "Advertised term must be exactly old term + 1")
      }
    }
  }

  // =====================================================================
  // 3. Adopting a HIGHER term from incoming RPCs
  // =====================================================================

  test("Follower adopts higher term from a granted VoteRequest") {
    val follower       = Follower(address = addrA, currentTerm = 3L)
    val req            = VoteRequest(addrB, candidateTerm = 10L, candidateLogIndex = -1L, candidateLastLogTerm = 0L)
    val (next, (r, _)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assertEquals(next.currentTerm, 10L, "Follower must adopt the candidate's higher term")
    assertEquals(r.term, 10L, "VoteResponse must echo the adopted term")
    assert(r.voteGranted)
  }

  test("Follower adopts higher term from AppendEntries") {
    val follower       = Follower(address = addrA, currentTerm = 3L)
    val req            = LogRequest(addrB, term = 9L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, _)) = follower.onLogRequest(req, emptyLog, None, cluster3)

    assertEquals(next.currentTerm, 9L, "Follower must adopt the leader's higher term")
    assertEquals(r.currentTerm, 9L)
  }

  test("Candidate adopts higher term from AppendEntries and steps down") {
    val cand           = Candidate(address = addrA, currentTerm = 5L)
    val req            = LogRequest(addrB, term = 9L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (_, _)) = cand.onLogRequest(req, emptyLog, None, cluster3)

    assert(next.isInstanceOf[Follower], "Candidate must step down for a higher-term leader")
    assertEquals(next.currentTerm, 9L)
  }

  test("Leader adopts higher term from AppendEntries and steps down") {
    val leader         = Leader(address = addrA, currentTerm = 4L)
    val req            = LogRequest(addrB, term = 7L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (_, a)) = leader.onLogRequest(req, emptyLog, None, cluster3)

    assert(next.isInstanceOf[Follower], "Leader must step down for a higher-term leader")
    assertEquals(next.currentTerm, 7L)
    assert(a.contains(StoreState), "Adopted term must be persisted")
  }

  test("Leader adopts higher term reported in a LogRequestResponse and steps down") {
    val leader = Leader(
      address = addrA,
      currentTerm = 3L,
      sentIndexMap = Map(addrB -> 2L, addrC -> 2L),
      ackIndexMap = Map(addrB  -> -1L, addrC -> -1L)
    )
    val msg          = LogRequestResponse(addrB, currentTerm = 10L, ackLogIndex = 2L, success = false)
    val (next, acts) = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    assert(next.isInstanceOf[Follower], "Leader must step down when a peer reports a higher term")
    assertEquals(next.currentTerm, 10L)
    assert(acts.contains(StoreState), "Adopted term must be persisted")
    assert(acts.contains(ResetLeaderAnnouncer))
  }

  test("Candidate adopts higher term from a granted VoteRequest and steps down") {
    val cand           = Candidate(address = addrA, currentTerm = 3L, votedFor = Some(addrA))
    val req            = VoteRequest(addrB, candidateTerm = 8L, candidateLogIndex = -1L, candidateLastLogTerm = 0L)
    val (next, (r, _)) = cand.onVoteRequest(req, emptyLog, cluster3)

    assert(next.isInstanceOf[Follower])
    assertEquals(next.currentTerm, 8L)
    assertEquals(r.term, 8L)
  }

  test("Leader adopts higher term from a VoteRequest with an up-to-date log and steps down") {
    val leader         = Leader(address = addrA, currentTerm = 3L)
    val log            = logAt(index = 2L, term = 2L)
    val req            = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = 2L, candidateLastLogTerm = 2L)
    val (next, (r, a)) = leader.onVoteRequest(req, log, cluster3)

    assert(next.isInstanceOf[Follower])
    assertEquals(next.currentTerm, 5L)
    assertEquals(r.term, 5L)
    assert(a.contains(StoreState))
  }

  // =====================================================================
  // 4. Never regressing to a LOWER term
  // =====================================================================

  test("Follower ignores a stale-term AppendEntries and keeps its term") {
    val follower       = Follower(address = addrA, currentTerm = 8L)
    val req            = LogRequest(addrB, term = 5L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, a)) = follower.onLogRequest(req, emptyLog, None, cluster3)

    assertEquals(next.currentTerm, 8L, "Term must not regress")
    assertEquals(next, follower, "State must be untouched")
    assertEquals(r.currentTerm, 8L, "Response must report the node's own higher term")
    assert(!r.success)
    assert(a.isEmpty)
  }

  test("Candidate ignores a stale-term AppendEntries and keeps its term") {
    val cand           = Candidate(address = addrA, currentTerm = 6L)
    val req            = LogRequest(addrB, term = 4L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, _)) = cand.onLogRequest(req, emptyLog, None, cluster3)

    assertEquals(next.currentTerm, 6L)
    assert(next.isInstanceOf[Candidate], "Candidate must not step down for a stale term")
    assert(!r.success)
  }

  test("Leader ignores a stale-term AppendEntries and keeps its term and role") {
    val leader         = Leader(address = addrA, currentTerm = 6L)
    val req            = LogRequest(addrB, term = 2L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, a)) = leader.onLogRequest(req, emptyLog, None, cluster3)

    assert(next.isInstanceOf[Leader], "Leader must not step down for a stale term")
    assertEquals(next.currentTerm, 6L)
    assertEquals(r.currentTerm, 6L)
    assert(!r.success)
    assert(a.isEmpty)
  }

  test("Follower rejecting a lower-term VoteRequest keeps its term") {
    val follower       = Follower(address = addrA, currentTerm = 7L, votedFor = Some(addrC))
    val req            = VoteRequest(addrB, candidateTerm = 4L, candidateLogIndex = 99L, candidateLastLogTerm = 99L)
    val (next, (r, a)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assertEquals(next.currentTerm, 7L, "Term must not regress on a stale VoteRequest")
    assertEquals(next, follower)
    assertEquals(r.term, 7L)
    assert(!r.voteGranted)
    assert(a.isEmpty)
  }

  test("Leader ignores a stale term reported in a LogRequestResponse") {
    val leader = Leader(
      address = addrA,
      currentTerm = 9L,
      sentIndexMap = Map(addrB -> 3L),
      ackIndexMap = Map(addrB  -> -1L)
    )
    val msg          = LogRequestResponse(addrB, currentTerm = 4L, ackLogIndex = -1L, success = false)
    val (next, _)    = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    assert(next.isInstanceOf[Leader], "Stale term in a response must not unseat the leader")
    assertEquals(next.currentTerm, 9L)
  }

  // =====================================================================
  // 5. Winning an election keeps the campaign term (no extra increment)
  // =====================================================================

  test("Candidate winning quorum becomes Leader in the SAME term") {
    val cand = Candidate(
      address = addrA,
      currentTerm = 6L,
      votedFor = Some(addrA),
      voteReceived = Set(addrA)
    )
    val (next, acts) = cand.onVoteResponse(VoteResponse(addrB, term = 6L, voteGranted = true), emptyLog, cluster3)

    assert(next.isInstanceOf[Leader], "Quorum reached, must become Leader")
    assertEquals(next.currentTerm, 6L, "Leader must serve the term it campaigned in, not term + 1")
    assert(acts.contains(AnnounceLeader(addrA)))
    acts.collect { case ReplicateLog(_, term, _) => term }
      .foreach(t => assertEquals(t, 6L, "Replication must be issued under the campaign term"))
  }

  test("full election round trip: term 0 Follower ends up Leader of term 1") {
    val follower                = Follower(address = addrA, currentTerm = 0L)
    val (candidate, electActs)  = follower.onTimer(emptyLog, cluster3)
    assertEquals(candidate.currentTerm, 1L)
    voteRequestTerms(electActs).foreach(t => assertEquals(t, 1L))

    val (leader, _) = candidate.onVoteResponse(VoteResponse(addrB, term = 1L, voteGranted = true), emptyLog, cluster3)
    assert(leader.isInstanceOf[Leader])
    assertEquals(leader.currentTerm, 1L, "Leader term must equal the election term")
  }

  test("Candidate does not change term while still collecting votes below quorum") {
    val cand = Candidate(address = addrA, currentTerm = 4L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val (next, acts) =
      cand.onVoteResponse(VoteResponse(addrB, term = 4L, voteGranted = false), emptyLog, cluster3)

    assert(next.isInstanceOf[Candidate])
    assertEquals(next.currentTerm, 4L, "A denied vote must not alter the term")
    assert(acts.isEmpty)
  }

  test("Candidate ignores a vote response from a different term") {
    val cand = Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val (next, acts) =
      cand.onVoteResponse(VoteResponse(addrB, term = 3L, voteGranted = true), emptyLog, cluster3)

    assert(next.isInstanceOf[Candidate], "A stale-term grant must not win the election")
    assertEquals(next.currentTerm, 5L)
    assert(acts.isEmpty)
  }

  // =====================================================================
  // 6. Term durability: persisted state always mirrors the current term
  // =====================================================================

  test("persisted state carries the incremented term right after starting an election") {
    val (candidate, actions) = Follower(address = addrA, currentTerm = 11L).onTimer(emptyLog, cluster3)

    assert(actions.contains(StoreState), "A term bump must be persisted before requesting votes")
    val ps = candidate.toPersistedState
    assertEquals(ps.term, 12L, "Persisted term must be the incremented term")
    assertEquals(ps.votedFor, Some(addrA), "Persisted vote must be the self-vote for the new term")
  }

  test("persisted state mirrors the term for every role") {
    assertEquals(Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrB)).toPersistedState.term, 5L)
    assertEquals(Candidate(address = addrA, currentTerm = 7L, votedFor = Some(addrA)).toPersistedState.term, 7L)
    assertEquals(Leader(address = addrA, currentTerm = 9L).toPersistedState.term, 9L)
  }

  test("term adopted from an RPC is persisted") {
    val follower  = Follower(address = addrA, currentTerm = 2L)
    val req       = VoteRequest(addrB, candidateTerm = 6L, candidateLogIndex = -1L, candidateLastLogTerm = 0L)
    val (next, (_, actions)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assert(actions.contains(StoreState), "An adopted term must be persisted")
    assertEquals(next.toPersistedState.term, 6L)
  }

  // =====================================================================
  // 7. Global invariant sweep: no transition may lower the term
  // =====================================================================

  test("invariant: no transition lowers the term for any role or message term") {
    val roles: List[Node] = List(
      Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrC)),
      Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA)),
      Leader(address = addrA, currentTerm = 5L, sentIndexMap = Map(addrB -> 1L), ackIndexMap = Map(addrB -> 0L))
    )
    val messageTerms = List(0L, 1L, 4L, 5L, 6L, 50L)
    val log          = logAt(index = 1L, term = 5L)

    roles.foreach { node =>
      messageTerms.foreach { t =>
        val onTimerTerm = node.onTimer(log, cluster3)._1.currentTerm
        assert(onTimerTerm >= node.currentTerm, s"onTimer lowered term for $node")

        val voteReq  = VoteRequest(addrB, candidateTerm = t, candidateLogIndex = 1L, candidateLastLogTerm = 5L)
        val voteTerm = node.onVoteRequest(voteReq, log, cluster3)._1.currentTerm
        assert(voteTerm >= node.currentTerm, s"onVoteRequest(term=$t) lowered term for $node")

        val voteRespTerm =
          node.onVoteResponse(VoteResponse(addrB, term = t, voteGranted = true), log, cluster3)._1.currentTerm
        assert(voteRespTerm >= node.currentTerm, s"onVoteResponse(term=$t) lowered term for $node")

        val logReq =
          LogRequest(addrB, term = t, prevSentLogIndex = 1L, prevLastLogTerm = 5L, entries = Nil, leaderCommit = 0L)
        val prevEntry = Some(LogEntry(term = 5L, index = 1L, command = NoOp))
        val logReqTerm = node.onLogRequest(logReq, log, prevEntry, cluster3)._1.currentTerm
        assert(logReqTerm >= node.currentTerm, s"onLogRequest(term=$t) lowered term for $node")

        val respTerm = node
          .onLogRequestResponse(log, cluster3, LogRequestResponse(addrB, currentTerm = t, ackLogIndex = 1L, success = true))
          ._1
          .currentTerm
        assert(respTerm >= node.currentTerm, s"onLogRequestResponse(term=$t) lowered term for $node")
      }
    }
  }

  // =====================================================================
  // 8. Term adoption is independent of vote granting (Raft Paper 5.1)
  // =====================================================================

  test("Follower at term 5 with no prior vote grants equal-term VoteRequest and records votedFor") {
    val follower       = Follower(address = addrA, currentTerm = 5L, votedFor = None)
    val req            = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = -1L, candidateLastLogTerm = 0L)
    val (next, (r, a)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assert(r.voteGranted, "Follower with no prior vote must grant an equal-term request with up-to-date log")
    assertEquals(next.currentTerm, 5L, "Term must remain 5 when candidateTerm equals currentTerm")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      Some(addrB),
      "votedFor must be set to the candidate after granting"
    )
    assert(a.contains(StoreState), "Granting a vote must persist state (Raft Paper 5.2)")
  }

  test("Follower retransmission: equal-term VoteRequest from same candidate is idempotent") {
    val follower       = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrB))
    val req            = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = -1L, candidateLastLogTerm = 0L)
    val (next, (r, a)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assert(r.voteGranted, "Re-sending a VoteRequest from the same candidate must be granted again (idempotent)")
    assertEquals(next.currentTerm, 5L, "Term must not change on a retransmission")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      Some(addrB),
      "votedFor must remain the same candidate after idempotent grant"
    )
    assert(a.contains(StoreState), "Idempotent grant must still persist state")
  }

  test("Follower at term 5 already voted for addrB denies equal-term VoteRequest from addrC") {
    val follower       = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrB))
    val req            = VoteRequest(addrC, candidateTerm = 5L, candidateLogIndex = 99L, candidateLastLogTerm = 99L)
    val (next, (r, a)) = follower.onVoteRequest(req, emptyLog, cluster3)

    assert(!r.voteGranted, "Follower that already voted for addrB must deny a different candidate in the same term")
    assertEquals(next.currentTerm, 5L, "Term must not change when denying a same-term request")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      Some(addrB),
      "votedFor must remain addrB after denying addrC"
    )
    assert(a.isEmpty, "No actions must be emitted when nothing changed (no term adoption, no grant)")
  }

  test("Follower adopts higher term from stale-log VoteRequest, denies vote, resets votedFor") {
    val follower       = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrC))
    val localLog       = logAt(3L, 5L)
    val req            = VoteRequest(addrB, candidateTerm = 9L, candidateLogIndex = 0L, candidateLastLogTerm = 1L)
    val (next, (r, a)) = follower.onVoteRequest(req, localLog, cluster3)

    assertEquals(
      next.currentTerm,
      9L,
      "Term must be adopted unconditionally even when the candidate log is stale (Raft Paper 5.1)"
    )
    assert(!r.voteGranted, "Vote must be denied when candidate log is stale (Raft Paper 5.4.1)")
    assertEquals(r.term, 9L, "VoteResponse must report the adopted term")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "votedFor must be reset to None when a higher term is adopted"
    )
    assert(a.contains(StoreState), "Adopted term must be persisted even when vote is denied")
  }

  test("Candidate adopts higher term from stale-log VoteRequest, steps down, denies vote, resets votedFor") {
    val candidate      = Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val localLog       = logAt(3L, 5L)
    val req            = VoteRequest(addrB, candidateTerm = 9L, candidateLogIndex = 0L, candidateLastLogTerm = 1L)
    val (next, (r, a)) = candidate.onVoteRequest(req, localLog, cluster3)

    assert(next.isInstanceOf[Follower], "Candidate must step down to Follower when a higher term is seen")
    assertEquals(
      next.currentTerm,
      9L,
      "Term must be adopted unconditionally even when the candidate log is stale (Raft Paper 5.1)"
    )
    assert(!r.voteGranted, "Vote must be denied when candidate log is stale (Raft Paper 5.4.1)")
    assertEquals(r.term, 9L, "VoteResponse must report the adopted term")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "votedFor must be reset to None when a higher term is adopted"
    )
    assert(a.contains(StoreState), "Adopted term must be persisted even when vote is denied")
  }

  test("Leader adopts higher term from stale-log VoteRequest, steps down, denies vote, emits StoreState and ResetLeaderAnnouncer") {
    val leader         = Leader(address = addrA, currentTerm = 5L)
    val localLog       = logAt(3L, 5L)
    val req            = VoteRequest(addrB, candidateTerm = 9L, candidateLogIndex = 0L, candidateLastLogTerm = 1L)
    val (next, (r, a)) = leader.onVoteRequest(req, localLog, cluster3)

    assert(next.isInstanceOf[Follower], "Leader must step down to Follower when a higher term is seen")
    assertEquals(
      next.currentTerm,
      9L,
      "Term must be adopted unconditionally even when the candidate log is stale (Raft Paper 5.1)"
    )
    assert(!r.voteGranted, "Vote must be denied when candidate log is stale (Raft Paper 5.4.1)")
    assertEquals(r.term, 9L, "VoteResponse must report the adopted term")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "votedFor must be reset to None when a higher term is adopted"
    )
    assert(a.contains(StoreState), "Adopted term must be persisted (Raft Paper 5.1)")
    assert(
      a.contains(ResetLeaderAnnouncer),
      "Leader stepping down must emit ResetLeaderAnnouncer to relinquish leadership"
    )
  }

  test("liveness: follower at term 9 after stale-log denial can still grant vote to legitimate term-9 candidate") {
    // Step 1: follower at term 5 receives a higher-term request with a stale log and adopts term 9
    val follower5      = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrC))
    val localLog       = logAt(3L, 5L)
    val staleReq       = VoteRequest(addrB, candidateTerm = 9L, candidateLogIndex = 0L, candidateLastLogTerm = 1L)
    val (follower9, _) = follower5.onVoteRequest(staleReq, localLog, cluster3)

    assertEquals(follower9.currentTerm, 9L, "Follower must have adopted term 9 after step 1")
    assertEquals(
      follower9.asInstanceOf[Follower].votedFor,
      None,
      "votedFor must be None after adopting a higher term"
    )

    // Step 2: a legitimate term-9 candidate with an up-to-date log now requests a vote
    val legitimateReq       = VoteRequest(addrC, candidateTerm = 9L, candidateLogIndex = 3L, candidateLastLogTerm = 5L)
    val (after, (resp, a2)) = follower9.onVoteRequest(legitimateReq, localLog, cluster3)

    assert(
      resp.voteGranted,
      "Follower at term 9 with votedFor=None must grant a vote to a legitimate term-9 candidate (liveness property)"
    )
    assertEquals(after.currentTerm, 9L, "Term must remain 9 after granting the vote")
    assertEquals(
      after.asInstanceOf[Follower].votedFor,
      Some(addrC),
      "votedFor must be set to addrC after granting"
    )
    assert(a2.contains(StoreState), "Granting a vote must persist state")
  }

  test("granted vote never sets currentLeader for Follower, Candidate, or Leader") {
    val localLog = logAt(3L, 5L)

    // Follower at term 5 grants a higher-term vote
    val follower           = Follower(address = addrA, currentTerm = 5L, votedFor = None)
    val followerReq        = VoteRequest(addrB, candidateTerm = 7L, candidateLogIndex = 3L, candidateLastLogTerm = 5L)
    val (followerNext, _)  = follower.onVoteRequest(followerReq, localLog, cluster3)
    assertEquals(
      followerNext.asInstanceOf[Follower].currentLeader,
      None,
      "Granting a vote must not set currentLeader: the candidate may still lose (Raft Paper 5.2)"
    )

    // Candidate at term 5 grants a higher-term vote and steps down
    val candidate          = Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val candidateReq       = VoteRequest(addrB, candidateTerm = 7L, candidateLogIndex = 3L, candidateLastLogTerm = 5L)
    val (candidateNext, _) = candidate.onVoteRequest(candidateReq, localLog, cluster3)
    assertEquals(
      candidateNext.asInstanceOf[Follower].currentLeader,
      None,
      "Candidate stepping down after granting a vote must not set currentLeader"
    )

    // Leader at term 5 grants a higher-term vote and steps down
    val leader          = Leader(address = addrA, currentTerm = 5L)
    val leaderReq       = VoteRequest(addrB, candidateTerm = 7L, candidateLogIndex = 3L, candidateLastLogTerm = 5L)
    val (leaderNext, _) = leader.onVoteRequest(leaderReq, localLog, cluster3)
    assertEquals(
      leaderNext.asInstanceOf[Follower].currentLeader,
      None,
      "Leader stepping down after granting a vote must not set currentLeader"
    )
  }

  test("Leader rejects equal-term LogRequest, stays Leader, emits no actions") {
    val leader         = Leader(address = addrA, currentTerm = 6L)
    val req            = LogRequest(addrB, term = 6L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, a)) = leader.onLogRequest(req, emptyLog, None, cluster3)

    assert(next.isInstanceOf[Leader], "Leader must not step down for an equal-term LogRequest")
    assertEquals(next.currentTerm, 6L, "Term must remain 6 when equal-term LogRequest is rejected")
    assert(!r.success, "Equal-term AppendEntries to a Leader must be rejected (two leaders in one term is impossible)")
    assertEquals(r.currentTerm, 6L, "Response must report the current term")
    assert(a.isEmpty, "No actions must be emitted when rejecting an equal-term LogRequest")
  }

  test("Candidate steps down to Follower on equal-term LogRequest (legitimate AppendEntries)") {
    val candidate      = Candidate(address = addrA, currentTerm = 6L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val req            = LogRequest(addrB, term = 6L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (r, a)) = candidate.onLogRequest(req, emptyLog, None, cluster3)

    assert(
      next.isInstanceOf[Follower],
      "Candidate must step down to Follower on equal-term AppendEntries (a winner has been elected)"
    )
    assertEquals(next.currentTerm, 6L, "Term must remain 6 after stepping down")
    assert(
      a.contains(AnnounceLeader(addrB)),
      "Candidate stepping down must announce the new leader"
    )
  }

  test("property: term adoption depends only on term comparison, never on log check") {
    val localLog = logAt(3L, 5L)

    val roles: List[Node] = List(
      Follower(address = addrA, currentTerm = 5L, votedFor = None),
      Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA)),
      Leader(address = addrA, currentTerm = 5L)
    )

    val candidateTerms = List(0L, 4L, 5L, 6L, 50L)

    val logVariants = List(
      (0L, 1L),  // stale candidate log
      (3L, 5L)   // up-to-date candidate log
    )

    roles.foreach { node =>
      candidateTerms.foreach { candidateTerm =>
        logVariants.foreach { case (logIndex, logTerm) =>
          val req              = VoteRequest(addrB, candidateTerm, logIndex, logTerm)
          val (next, (_, _))   = node.onVoteRequest(req, localLog, cluster3)
          val expectedTerm     = math.max(5L, candidateTerm)
          assertEquals(
            next.currentTerm,
            expectedTerm,
            s"Term adoption must depend only on term comparison: role=${node.getClass.getSimpleName}, " +
              s"candidateTerm=$candidateTerm, logIndex=$logIndex, logTerm=$logTerm. " +
              s"Expected term=$expectedTerm but got ${next.currentTerm}"
          )
        }
      }
    }
  }
}
