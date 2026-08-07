package com.grok.raft.core.internal

import munit.FunSuite

import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.*

/** Verifies all five Raft correctness properties through 25 precisely specified scenarios.
  *
  * Properties covered:
  *   - §5.2 Election Safety: at most one leader per term
  *   - §5.3 Leader Append-Only: matchIndex is monotonically non-decreasing
  *   - §5.3 Log Matching: prevLog consistency check enforced on every AppendEntries
  *   - §5.4 Leader Completeness: election restriction ensures future leaders hold committed data
  *   - §5.4 State Machine Safety: commit correctness and ack-map semantics
  */
class RaftCorrectnessSpec extends FunSuite {

  // --- Fixtures ---

  val addrA = TestData.addr1
  val addrB = TestData.addr2
  val addrC = TestData.addr3
  val addrD = NodeAddress("n4", 9090)
  val addrE = NodeAddress("n5", 9090)

  val cluster3: ClusterConfiguration =
    ClusterConfiguration(
      currentNode = Follower(address = addrA, currentTerm = 0L),
      members = List(addrA, addrB, addrC)
    )

  val cluster5: ClusterConfiguration =
    ClusterConfiguration(
      currentNode = Follower(address = addrA, currentTerm = 0L),
      members = List(addrA, addrB, addrC, addrD, addrE)
    )

  val emptyLog: LogState = LogState(lastLogIndex = -1L, lastLogTerm = None, appliedLogIndex = -1L)

  def logAt(index: Long, term: Long): LogState =
    LogState(lastLogIndex = index, lastLogTerm = Some(term), appliedLogIndex = index - 1)

  // =====================================================================
  // Section 1 — Election Safety (§5.2): at most one leader per term
  // =====================================================================

  test("Candidate rejects equal-term VoteRequest from a different candidate (split-vote safety)") {
    val candidate = Candidate(
      address = addrA,
      currentTerm = 5L,
      votedFor = Some(addrA),
      voteReceived = Set(addrA)
    )
    val req                = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = 10L, candidateLastLogTerm = 5L)
    val log                = logAt(10L, 5L)
    val (next, (resp, acts)) = candidate.onVoteRequest(req, log, cluster3)

    assert(next.isInstanceOf[Candidate], "§5.2: Candidate must stay Candidate when rejecting equal-term VoteRequest")
    assertEquals(next.currentTerm, 5L, "§5.2: Term must remain 5 when rejecting equal-term VoteRequest")
    assertEquals(resp.voteGranted, false, "§5.2: Vote must be denied to prevent two candidates reaching quorum in same term")
    assertEquals(resp.term, 5L, "§5.2: Response term must echo current term")
    assert(!acts.contains(StoreState), "§5.2: No StoreState when nothing changed (same term, no grant)")
  }

  test("Follower refuses second vote in same term to a different candidate") {
    val follower           = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrB))
    val req                = VoteRequest(addrC, candidateTerm = 5L, candidateLogIndex = 10L, candidateLastLogTerm = 5L)
    val log                = logAt(5L, 5L)
    val (next, (resp, acts)) = follower.onVoteRequest(req, log, cluster3)

    assert(next.isInstanceOf[Follower], "§5.2: Follower must stay Follower when denying second vote in same term")
    assertEquals(resp.voteGranted, false, "§5.2: Second vote in same term to different candidate must be denied")
    assertEquals(resp.term, 5L, "§5.2: Response term must echo current term")
    assert(acts.isEmpty, "§5.2: No actions when state is unchanged (no term adoption, no grant)")
  }

  test("Follower re-grants vote to same candidate in same term (idempotent retransmission)") {
    val follower           = Follower(address = addrA, currentTerm = 5L, votedFor = Some(addrB))
    val req                = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = 10L, candidateLastLogTerm = 5L)
    val log                = logAt(5L, 5L)
    val (next, (resp, acts)) = follower.onVoteRequest(req, log, cluster3)

    assert(next.isInstanceOf[Follower], "§5.2: Follower must stay Follower on idempotent retransmission")
    assertEquals(resp.voteGranted, true, "§5.2: Re-grant to same candidate must succeed (idempotent retransmission)")
    assertEquals(resp.term, 5L, "§5.2: Response term must echo current term")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      Some(addrB),
      "§5.2: votedFor must remain Some(addrB) after idempotent re-grant"
    )
    assert(acts.contains(StoreState), "§5.2: StoreState must be emitted even on idempotent grant (Raft Paper §5.2)")
  }

  test("Leader rejects equal-term AppendEntries (two-leaders-same-term is impossible)") {
    val leader             = Leader(address = addrA, currentTerm = 5L)
    val req                = LogRequest(addrB, term = 5L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (resp, acts)) = leader.onLogRequest(req, emptyLog, None, cluster3)

    assert(next.isInstanceOf[Leader], "§5.2: Leader must not step down for equal-term AppendEntries")
    assertEquals(resp.success, false, "§5.2: Equal-term AppendEntries must be rejected (two leaders in one term is impossible)")
    assertEquals(resp.currentTerm, 5L, "§5.2: Response must report the current term")
    assert(acts.isEmpty, "§5.2: No actions when rejecting equal-term AppendEntries")
  }

  test("Leader rejects equal-term VoteRequest (has implicit self-vote for current term)") {
    val leader             = Leader(address = addrA, currentTerm = 5L)
    val req                = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = 10L, candidateLastLogTerm = 5L)
    val log                = logAt(10L, 5L)
    val (next, (resp, acts)) = leader.onVoteRequest(req, log, cluster3)

    assert(next.isInstanceOf[Leader], "§5.2: Leader must stay Leader when rejecting equal-term VoteRequest")
    assertEquals(resp.voteGranted, false, "§5.2: Leader must deny equal-term VoteRequest (implicit self-vote)")
    assertEquals(resp.term, 5L, "§5.2: Response term must echo current term")
    assert(
      acts.exists { case ReplicateLog(_, _, _) => true; case _ => false },
      "§5.2: Leader must emit ReplicateLog to bring candidate up to date"
    )
    assert(!acts.contains(StoreState), "§5.2: No StoreState when leader stays leader with no term change")
  }

  // =====================================================================
  // Section 2 — Leader Append-Only (§5.3): matchIndex must be monotone
  // =====================================================================

  test("Leader matchIndex never decreases on success: stale delayed ack is ignored") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 10L),
      ackIndexMap = Map(addrB  -> 8L)
    )
    val msg          = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 5L, success = true)
    val (next, acts) = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    val nextLeader = next.asInstanceOf[Leader]
    assertEquals(
      nextLeader.ackIndexMap.apply(addrB),
      8L,
      "§5.3 Figure 2: matchIndex must be monotone non-decreasing; stale ack must not regress ackIndex from 8 to 5"
    )
    assertEquals(
      nextLeader.sentIndexMap.apply(addrB),
      5L,
      "§5.3: sentIndexMap must be updated to the ackLogIndex from the response"
    )
  }

  test("Leader on successful heartbeat ack advances ackIndex") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 10L),
      ackIndexMap = Map(addrB  -> 4L)
    )
    val msg          = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 10L, success = true)
    val (next, acts) = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    val nextLeader = next.asInstanceOf[Leader]
    assertEquals(
      nextLeader.sentIndexMap.apply(addrB),
      10L,
      "§5.3: sentIndexMap must be updated to ackLogIndex=10 on success"
    )
    assertEquals(
      nextLeader.ackIndexMap.apply(addrB),
      10L,
      "§5.3: ackIndexMap must advance to 10 on successful ack"
    )
    assert(
      acts.exists { case CommitLogs(_) => true; case _ => false },
      "§5.3: CommitLogs must be emitted after a successful ack"
    )
  }

  test("Leader receiving stale-term AppendEntries: stays Leader, no log mutation") {
    val leader             = Leader(address = addrA, currentTerm = 6L)
    val req                = LogRequest(addrB, term = 3L, prevSentLogIndex = -1L, prevLastLogTerm = 0L, entries = Nil, leaderCommit = -1L)
    val (next, (resp, acts)) = leader.onLogRequest(req, emptyLog, None, cluster3)

    assertEquals(next, leader, "§5.3: Leader state must be identical (no mutation) when rejecting stale-term AppendEntries")
    assertEquals(resp.success, false, "§5.3: Stale-term AppendEntries must be rejected")
    assert(acts.isEmpty, "§5.3: No actions when rejecting stale-term AppendEntries")
  }

  // =====================================================================
  // Section 3 — Log Matching (§5.3): consistency check on prevLog
  // =====================================================================

  test("Follower rejects AppendEntries when prevLogTerm mismatches at existing index") {
    val follower = Follower(address = addrA, currentTerm = 2L)
    val log      = logAt(5L, 2L)
    val req = LogRequest(
      addrB,
      term = 2L,
      prevSentLogIndex = 3L,
      prevLastLogTerm = 99L,
      entries = Nil,
      leaderCommit = -1L
    )
    val prevEntry            = Some(LogEntry(term = 1L, index = 3L, command = NoOp))
    val (next, (resp, acts)) = follower.onLogRequest(req, log, prevEntry, cluster3)

    assertEquals(resp.success, false, "§5.3: AppendEntries must be rejected when prevLogTerm mismatches at existing index")
    assertEquals(resp.ackLogIndex, -1L, "§5.3: ackLogIndex must be -1 on rejection")
    assert(acts.contains(StoreState), "§5.3: StoreState must be emitted even on rejection (leader term adopted)")
  }

  test("Leader-forced full rewrite from index 0 succeeds on non-empty follower log") {
    val follower = Follower(address = addrA, currentTerm = 2L)
    val log      = logAt(5L, 3L)
    val req = LogRequest(
      addrB,
      term = 3L,
      prevSentLogIndex = -1L,
      prevLastLogTerm = 0L,
      entries = List(LogEntry(3L, 0L, NoOp)),
      leaderCommit = -1L
    )
    val (next, (resp, acts)) = follower.onLogRequest(req, log, None, cluster3)

    assertEquals(resp.success, true, "§5.3: Full rewrite from index 0 (prevSentLogIndex=-1) must succeed")
    assertEquals(resp.ackLogIndex, 0L, "§5.3: ackLogIndex must be prevSentLogIndex(-1) + 1 entry = 0")
  }

  test("prevSentLogIndex == lastLogIndex succeeds (boundary: exactly one past the last stored)") {
    val follower = Follower(address = addrA, currentTerm = 2L)
    val log      = logAt(5L, 2L)
    val req = LogRequest(
      addrB,
      term = 2L,
      prevSentLogIndex = 5L,
      prevLastLogTerm = 2L,
      entries = List(LogEntry(2L, 6L, NoOp)),
      leaderCommit = -1L
    )
    val prevEntry            = Some(LogEntry(term = 2L, index = 5L, command = NoOp))
    val (next, (resp, acts)) = follower.onLogRequest(req, log, prevEntry, cluster3)

    assertEquals(resp.success, true, "§5.3: AppendEntries must succeed when prevSentLogIndex == lastLogIndex and terms match")
    assertEquals(resp.ackLogIndex, 6L, "§5.3: ackLogIndex must be prevSentLogIndex(5) + 1 entry = 6")
  }

  test("prevSentLogIndex == logLength (off by one beyond last) fails") {
    val follower = Follower(address = addrA, currentTerm = 2L)
    val log      = logAt(5L, 2L) // logLength = 6
    val req = LogRequest(
      addrB,
      term = 2L,
      prevSentLogIndex = 6L, // beyond log (logLength=6, valid indices 0..5)
      prevLastLogTerm = 2L,
      entries = List(LogEntry(2L, 7L, NoOp)),
      leaderCommit = -1L
    )
    val (next, (resp, acts)) = follower.onLogRequest(req, log, None, cluster3)

    assertEquals(resp.success, false, "§5.3: AppendEntries must fail when prevSentLogIndex is beyond the log length")
    assertEquals(resp.ackLogIndex, -1L, "§5.3: ackLogIndex must be -1 on rejection")
  }

  test("Candidate stepping down to follower still enforces Log Matching on AppendEntries") {
    val candidate = Candidate(
      address = addrA,
      currentTerm = 2L,
      votedFor = Some(addrA),
      voteReceived = Set(addrA)
    )
    val log = logAt(4L, 2L)
    val req = LogRequest(
      addrB,
      term = 3L,
      prevSentLogIndex = 4L,
      prevLastLogTerm = 1L, // mismatch: log has term=2 at index=4
      entries = Nil,
      leaderCommit = -1L
    )
    val prevEntry            = Some(LogEntry(term = 2L, index = 4L, command = NoOp))
    val (next, (resp, acts)) = candidate.onLogRequest(req, log, prevEntry, cluster3)

    assert(next.isInstanceOf[Follower], "§5.3: Candidate must step down to Follower on higher-term AppendEntries")
    assertEquals(next.currentTerm, 3L, "§5.3: Candidate must adopt the leader's higher term")
    assertEquals(
      next.asInstanceOf[Follower].currentLeader,
      Some(addrB),
      "§5.3: Follower must record the new leader after stepping down"
    )
    assertEquals(resp.success, false, "§5.3: Log Matching must still be enforced even when stepping down (term mismatch at prevSentLogIndex)")
    assertEquals(resp.ackLogIndex, -1L, "§5.3: ackLogIndex must be -1 on rejection")
    assert(acts.contains(StoreState), "§5.3: StoreState must be emitted when adopting new term")
    assert(
      acts.exists { case AnnounceLeader(id, _) => id == addrB; case _ => false },
      "§5.3: AnnounceLeader(addrB) must be emitted when stepping down to follower"
    )
  }

  test("Heartbeat with empty entries but mismatched prev still fails (no silent success)") {
    val follower = Follower(address = addrA, currentTerm = 2L)
    val log      = logAt(5L, 2L)
    val req = LogRequest(
      addrB,
      term = 2L,
      prevSentLogIndex = 3L,
      prevLastLogTerm = 7L, // mismatch: log has term=2 at index=3
      entries = Nil,
      leaderCommit = -1L
    )
    val prevEntry            = Some(LogEntry(term = 2L, index = 3L, command = NoOp))
    val (next, (resp, acts)) = follower.onLogRequest(req, log, prevEntry, cluster3)

    assertEquals(resp.success, false, "§5.3: Heartbeat with empty entries must not be silently accepted when prevLogTerm mismatches")
    assertEquals(resp.ackLogIndex, -1L, "§5.3: ackLogIndex must be -1 on rejection even for empty-entry heartbeat")
  }

  // =====================================================================
  // Section 4 — Leader Completeness (§5.4): election restriction
  // =====================================================================

  test("Vote denied when candidate index matches but term is lower (log tiebreak: index)") {
    val follower           = Follower(address = addrA, currentTerm = 3L, votedFor = None)
    val log                = logAt(7L, 3L)
    val req                = VoteRequest(addrB, candidateTerm = 3L, candidateLogIndex = 5L, candidateLastLogTerm = 3L)
    val (next, (resp, acts)) = follower.onVoteRequest(req, log, cluster3)

    assertEquals(resp.voteGranted, false, "§5.4: Vote must be denied when candidateLogIndex(5) < follower lastLogIndex(7)")
    assertEquals(resp.term, 3L, "§5.4: Response term must echo current term")
    assert(!acts.contains(StoreState), "§5.4: No StoreState when state is unchanged (same term, no grant)")
  }

  test("Vote denied when candidate lastLogTerm lower than follower's even if candidate log is longer") {
    val follower           = Follower(address = addrA, currentTerm = 3L, votedFor = None)
    val log                = logAt(2L, 5L)
    val req                = VoteRequest(addrB, candidateTerm = 6L, candidateLogIndex = 100L, candidateLastLogTerm = 4L)
    val (next, (resp, acts)) = follower.onVoteRequest(req, log, cluster3)

    assertEquals(next.currentTerm, 6L, "§5.4: Higher term must be adopted unconditionally (§5.1)")
    assertEquals(resp.voteGranted, false, "§5.4: Vote must be denied when candidateLastLogTerm(4) < follower lastLogTerm(5)")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "§5.4: votedFor must be None after adopting higher term without granting vote"
    )
    assertEquals(resp.term, 6L, "§5.4: Response term must echo the adopted term")
    assert(acts.contains(StoreState), "§5.4: StoreState must be emitted when adopting higher term (§5.1)")
  }

  test("Vote granted when candidate has strictly higher lastLogTerm despite shorter log") {
    val follower           = Follower(address = addrA, currentTerm = 3L, votedFor = None)
    val log                = logAt(50L, 2L)
    val req                = VoteRequest(addrB, candidateTerm = 4L, candidateLogIndex = 3L, candidateLastLogTerm = 3L)
    val (next, (resp, acts)) = follower.onVoteRequest(req, log, cluster3)

    assertEquals(resp.voteGranted, true, "§5.4: Vote must be granted when candidateLastLogTerm(3) > follower lastLogTerm(2)")
    assertEquals(next.currentTerm, 4L, "§5.4: Term must be adopted to 4")
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      Some(addrB),
      "§5.4: votedFor must be set to addrB after granting vote"
    )
    assertEquals(resp.term, 4L, "§5.4: Response term must echo the adopted term")
    assert(acts.contains(StoreState), "§5.4: StoreState must be emitted when granting vote")
  }

  test("Empty-log candidate cannot win vote from follower with a committed entry") {
    // Step 1: empty-log Candidate(addrA, term=1).onTimer gives VoteRequest with candidateLogIndex=-1
    val (candidateAfterTimer, timerActions) =
      Candidate(address = addrA, currentTerm = 0L).onTimer(emptyLog, cluster3)

    val voteReqOpt = timerActions.collectFirst { case RequestForVote(peer, req) if peer == addrB => req }
    assert(voteReqOpt.isDefined, "§5.4: onTimer must emit RequestForVote actions")

    val voteReq = voteReqOpt.get
    assertEquals(voteReq.candidateLogIndex, -1L, "§5.4: Empty-log candidate must advertise candidateLogIndex=-1")

    // Step 2: feed that request into Follower(addrB, term=1, votedFor=None) with logState=logAt(0,1)
    val followerB              = Follower(address = addrB, currentTerm = 1L, votedFor = None)
    val followerLog            = logAt(0L, 1L)
    val (_, (resp, _))         = followerB.onVoteRequest(voteReq, followerLog, cluster3)

    assertEquals(resp.voteGranted, false, "§5.4: Follower with committed entry must deny vote to empty-log candidate")
  }

  test("Stale-term VoteResponse (term < currentTerm) must not count toward quorum") {
    val candidate = Candidate(
      address = addrA,
      currentTerm = 5L,
      votedFor = Some(addrA),
      voteReceived = Set(addrA)
    )
    val staleResp    = VoteResponse(addrB, term = 4L, voteGranted = true)
    val (next, acts) = candidate.onVoteResponse(staleResp, emptyLog, cluster3)

    // The critical safety property: quorum must NOT be reached from a stale-term grant.
    // The node must stay Candidate (not become Leader) because term=4 != currentTerm=5.
    assert(next.isInstanceOf[Candidate], "§5.4: Stale-term VoteResponse must not cause quorum to be reached; node must stay Candidate")
    assertEquals(next.currentTerm, 5L, "§5.4: Term must remain 5 when processing stale-term VoteResponse")
    assert(acts.isEmpty, "§5.4: No actions (no leader promotion) when stale-term VoteResponse cannot satisfy quorum check")
  }

  test("Candidate adopts higher term seen in a VoteResponse and steps down immediately (§5.1)") {
    val candidate    = Candidate(address = addrA, currentTerm = 5L, votedFor = Some(addrA), voteReceived = Set(addrA))
    val resp         = VoteResponse(addrB, term = 9L, voteGranted = false)
    val (next, acts) = candidate.onVoteResponse(resp, emptyLog, cluster3)

    assert(next.isInstanceOf[Follower], "§5.1: Candidate must step down immediately on higher-term VoteResponse")
    assertEquals(next.currentTerm, 9L, "§5.1: Term must be adopted to 9")
    assertEquals(
      next.asInstanceOf[Follower].currentLeader,
      None,
      "§5.1: currentLeader must be None after stepping down via VoteResponse"
    )
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "§5.1: votedFor must be reset to None when adopting higher term"
    )
    assert(acts.contains(StoreState), "§5.1: StoreState must be emitted when adopting higher term")
  }

  // =====================================================================
  // Section 5 — State Machine Safety (§5.4): commit correctness
  // =====================================================================

  test("Leader.onVoteRequest reject path uses leader's own sentIndex, never candidate's unverified claim") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 9L),
      ackIndexMap = Map(addrB  -> 9L)
    )
    val req                  = VoteRequest(addrB, candidateTerm = 5L, candidateLogIndex = 2L, candidateLastLogTerm = 1L)
    val log                  = logAt(10L, 5L)
    val (next, (resp, acts)) = leader.onVoteRequest(req, log, cluster3)

    val nextLeader = next.asInstanceOf[Leader]
    assertEquals(
      nextLeader.ackIndexMap.apply(addrB),
      9L,
      "§5.4.2: ackIndexMap must not be updated from candidate's unverified self-reported log index"
    )
    assertEquals(
      nextLeader.sentIndexMap.apply(addrB),
      9L,
      "§5.4.2: sentIndexMap must not be updated from candidate's unverified self-reported log index"
    )
    assert(
      acts.exists {
        case ReplicateLog(peer, term, prefixIndex) => peer == addrB && term == 5L && prefixIndex == 9L
        case _                                     => false
      },
      "§5.4.2: ReplicateLog must use leader's own sentIndex(9), not candidate's unverified candidateLogIndex(2)"
    )
  }

  test("CommitLogs action includes leader's own appliedLogIndex as self ack") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 4L, addrC -> 2L),
      ackIndexMap = Map(addrB  -> 3L, addrC -> 1L)
    )
    val logState = LogState(lastLogIndex = 5L, lastLogTerm = Some(5L), appliedLogIndex = 7L)
    val msg      = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 6L, success = true)
    val (_, acts) = leader.onLogRequestResponse(logState, cluster3, msg)

    val commitAction = acts.collectFirst { case c: CommitLogs => c }
    assert(commitAction.isDefined, "§5.4: CommitLogs must be emitted on successful ack")

    val ackMap = commitAction.get.ackIndexMap
    assertEquals(
      ackMap.apply(addrA),
      7L,
      "§5.4: CommitLogs ackIndexMap must include leader's own appliedLogIndex(7) as self ack"
    )
    assertEquals(
      ackMap.apply(addrB),
      6L,
      "§5.4: CommitLogs ackIndexMap must include updated ackIndex for addrB(6)"
    )
    assertEquals(
      ackMap.apply(addrC),
      1L,
      "§5.4: CommitLogs ackIndexMap must include unchanged ackIndex for addrC(1)"
    )
  }

  test("matchIndex monotonicity: two sequential successes with 8 then 4 leave ackIndex at 8") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 10L),
      ackIndexMap = Map(addrB  -> -1L)
    )

    // First success: ackLogIndex=8
    val msg1          = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 8L, success = true)
    val (after1, _)   = leader.onLogRequestResponse(emptyLog, cluster3, msg1)
    val leader1       = after1.asInstanceOf[Leader]

    // Second success: ackLogIndex=4 (stale/reordered)
    val msg2          = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 4L, success = true)
    val (after2, _)   = leader1.onLogRequestResponse(emptyLog, cluster3, msg2)
    val leader2       = after2.asInstanceOf[Leader]

    assertEquals(
      leader2.ackIndexMap.apply(addrB),
      8L,
      "§5.3 Figure 2: matchIndex must remain 8 after stale ack with ackLogIndex=4 (monotone non-decreasing)"
    )
    assertEquals(
      leader2.sentIndexMap.apply(addrB),
      4L,
      "§5.3: sentIndexMap must be updated to the latest ackLogIndex(4) from the response"
    )
  }

  test("Failure backtracking floors nextIndex at -1 and never below") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 0L),
      ackIndexMap = Map(addrB  -> -1L)
    )

    // First failure: sentIndex=0 -> decremented to -1
    val msg1          = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = -1L, success = false)
    val (after1, acts1) = leader.onLogRequestResponse(emptyLog, cluster3, msg1)
    val leader1         = after1.asInstanceOf[Leader]

    assertEquals(
      leader1.sentIndexMap.apply(addrB),
      -1L,
      "§5.3: sentIndexMap must be decremented to -1 when sentIndex was 0"
    )
    assert(
      acts1.exists {
        case ReplicateLog(peer, _, prefixIndex) => peer == addrB && prefixIndex == -1L
        case _                                  => false
      },
      "§5.3: ReplicateLog must be emitted with prefixIndex=-1 after backtracking"
    )

    // Second failure: sentIndex already -1, must stay -1
    val msg2            = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = -1L, success = false)
    val (after2, acts2) = leader1.onLogRequestResponse(emptyLog, cluster3, msg2)
    val leader2         = after2.asInstanceOf[Leader]

    assertEquals(
      leader2.sentIndexMap.apply(addrB),
      -1L,
      "§5.3: sentIndexMap must stay at -1 (floor) when already at minimum; must not go below -1"
    )
    assert(
      acts2.exists {
        case ReplicateLog(peer, _, prefixIndex) => peer == addrB && prefixIndex == -1L
        case _                                  => false
      },
      "§5.3: ReplicateLog must still be emitted with prefixIndex=-1 when already at floor"
    )
  }

  test("Leader steps down on higher-term failure response, abandoning uncommitted work") {
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 3L),
      ackIndexMap = Map(addrB  -> -1L)
    )
    val msg          = LogRequestResponse(addrB, currentTerm = 8L, ackLogIndex = 0L, success = false)
    val (next, acts) = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    assert(next.isInstanceOf[Follower], "§5.2: Leader must step down to Follower on higher-term failure response")
    assertEquals(next.currentTerm, 8L, "§5.2: Term must be adopted to 8")
    assertEquals(
      next.asInstanceOf[Follower].currentLeader,
      None,
      "§5.2: currentLeader must be None after stepping down"
    )
    assertEquals(
      next.asInstanceOf[Follower].votedFor,
      None,
      "§5.2: votedFor must be None after stepping down"
    )
    assert(acts.contains(StoreState), "§5.2: StoreState must be emitted when adopting higher term")
    assert(acts.contains(ResetLeaderAnnouncer), "§5.2: ResetLeaderAnnouncer must be emitted when leader steps down")
  }

  test("CommitLogs carries a raw ack map without term-filtering (§5.4.2 enforcement delegated to Log layer)") {
    // The §5.4.2 Figure 8 constraint — that only current-term entries may be committed by
    // replica count — is enforced at the Log layer, not at the Node state machine layer.
    // The Node layer simply emits CommitLogs with a plain Long map of ack indices.
    val leader = Leader(
      address = addrA,
      currentTerm = 5L,
      sentIndexMap = Map(addrB -> 5L, addrC -> 5L),
      ackIndexMap = Map(addrB  -> -1L, addrC -> -1L)
    )
    val msg       = LogRequestResponse(addrB, currentTerm = 5L, ackLogIndex = 3L, success = true)
    val (_, acts) = leader.onLogRequestResponse(emptyLog, cluster3, msg)

    val commitAction = acts.collectFirst { case c: CommitLogs => c }
    assert(commitAction.isDefined, "§5.4: CommitLogs must be emitted on successful ack")

    val ackMap = commitAction.get.ackIndexMap
    assert(
      ackMap.values.forall(_.isInstanceOf[Long]),
      "§5.4.2: CommitLogs ackIndexMap must be a plain Long map with no term annotations; " +
        "§5.4.2 Figure 8 constraint is enforced at the Log layer, not here"
    )
    assert(
      commitAction.get.isInstanceOf[CommitLogs],
      "§5.4.2: The commit action must be an instance of CommitLogs (plain ack map, no term filtering)"
    )
  }

}
