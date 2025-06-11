package com.grok.raft.core.internal

import cats.effect.*
import cats.effect.kernel.*
import cats.implicits.*
import cats.*

// Bring your domain types into scope
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.internal.*
import com.grok.raft.core.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.PersistedState
import com.grok.raft.core.storage.StateStorage

object NoOp extends ReadCommand[Unit]


object TestData {

  val addr1 = NodeAddress("n1", 9090)
  val addr2 = NodeAddress("n2", 9090)
  val addr3 = NodeAddress("n3", 9090)

  val leader = Leader(currentTerm = 1L, address = addr1)
  val follower1 = Follower(currentTerm = 1L, address = addr2)
  val follower2 = Follower(currentTerm = 1L, address = addr3)

}

// 1) An in‐memory LogStorage
class InMemoryLogStorage[F[_]: Sync] extends LogStorage[F] {

  override def deleteBefore(index: Long): F[Unit] = ???

  private val ref = Ref.unsafe[F, Map[Long, LogEntry]](Map.empty)

  override def get(index: Long): F[Option[LogEntry]] =
    ref.get.map(_.get(index))

  override def put(index: Long, entry: LogEntry): F[LogEntry] =
    ref.update(_ + (index -> entry)).as(entry)

  override def deleteAfter(index: Long): F[Unit] =
    ref.update(_.filter { case (k, _) => k <= index })

  override def currentLength: F[Long] =
    ref.get.map(_.size.toLong)
}

class InMemoryStateMachine[F[_]: Sync] extends StateMachine[F] {
  // In‐memory state machine that does nothing

  override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = { _ => Sync[F].pure(true) }

  override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = { _ => Sync[F].pure(true) }

  override def appliedIndex: F[Long] = Sync[F].pure(0L)

}

class DummyMembershipManager[F[_]: Sync] extends MembershipManager[F]:

  val configurationRef: Ref[F, ClusterConfiguration] =
    Ref.unsafe[F, ClusterConfiguration](
      ClusterConfiguration(
        currentNode = TestData.leader ,
        members = List(TestData.addr1, TestData.addr2, TestData.addr3)
      )
    )

  override def members: F[Set[Node]] = Monad[F].pure(
    Set(
      TestData.leader,
      TestData.follower1,
      TestData.follower2
    )
  )

  override def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] = configurationRef.set(newConfig)
  override def getClusterConfiguration: F[ClusterConfiguration] = configurationRef.get

given logger: Logger[IO] = Slf4jLogger.getLogger[IO]




// ----------------------------------------------------------------
// STUB LEADER ANNOUNCER
// ----------------------------------------------------------------
class StubLeaderAnnouncer[F[_]: Concurrent] private (deferred: Deferred[F, NodeAddress])
    extends LeaderAnnouncer[F] {

  override def announce(leader: NodeAddress): F[Unit] =
    // Complete only once; further completes are no‐ops
    deferred.complete(leader).void

  override def listen(): F[NodeAddress] =
    // Block until `announce` is called
    deferred.get

  override def reset(): F[Unit] =
    // Not used in our tests
    Concurrent[F].unit
}

object StubLeaderAnnouncer {
  def create[F[_]: Concurrent]: F[StubLeaderAnnouncer[F]] =
    Deferred[F, NodeAddress].map(new StubLeaderAnnouncer[F](_))
}



class StubRpcClient[F[_]: Sync](voteMap: Map[NodeAddress, Boolean]) extends RpcClient[F] {

  override def send[T](serverId: NodeAddress, command: Command[T]): F[T] = ???

  override def join(serverId: NodeAddress, newNode: NodeAddress): F[Boolean] = ???

  override def closeConnections(): F[Unit] = ???

  override def send(peer: NodeAddress, req: VoteRequest): F[VoteResponse] =
    Sync[F].delay {
      VoteResponse(peer, req.candidateTerm, voteMap.getOrElse(peer, false))
    }

  override def send(peer: NodeAddress, req: LogRequest): F[LogRequestResponse] =
    Sync[F].pure(LogRequestResponse(peer, req.term, req.entries.length, true))
}

class DummyLogPropagator[F[_]: Sync] extends LogPropagator[F] {
  override def propagateLogs(
      peer: NodeAddress,
      term: Long,
      prefixLength: Long
  ): F[LogRequestResponse] =
    Sync[F].pure(LogRequestResponse(peer, term,  prefixLength, success = true))
}

class InMemoryLog[F[_]: Sync] extends Log[F] {
    override val logStorage = new InMemoryLogStorage[F]
    // these are never called in our tests, so null is okay
    override val membershipManager = new DummyMembershipManager[F]
    override val stateMachine      = new InMemoryStateMachine[F]

    // identity transaction
    override def transactional[A](t: => F[A]): F[A] = t

    // commit‐index stored in a Ref so we can observe it if we wanted
    private val commitRef                           = Ref.unsafe[F, Long](0L)
    override def getCommittedLength: F[Long]       = commitRef.get
    override def setCommitLength(i: Long): F[Unit] = commitRef.set(i)

    // Not used in these tests
    override def state: F[LogState] = Sync[F].pure(LogState(0, None, 0))

    // Stubs for the methods referenced by appendEntries (not needed here)
    def compactLogs(): F[Unit] = Sync[F].unit
}

class InMemoryStateStorage [F[_]: Sync] extends StateStorage[F] {

  private val ref = Ref.unsafe[F, PersistedState](PersistedState(0L, None, 0L))

  def persistState(state: PersistedState): F[Unit] = ref.set(state)

  def retrieveState: F[Option[PersistedState]] = 

    ref.get.map(Some(_))
}