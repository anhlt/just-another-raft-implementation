package com.grok.raft.core.internal

import cats.effect.*
import cats.effect.kernel.Ref
import munit.CatsEffectSuite
import cats.implicits.*
import cats.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import cats.effect.kernel.{Deferred => EffectDeferred}

// Bring your domain types into scope
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.internal.StateMachine
import com.grok.raft.core._
import com.grok.raft.core.internal.{Node, Leader, NodeAddress}
import com.grok.raft.core.internal.Deferred

import com.grok.raft.core.error.Error
given ioMonadErrorForError: MonadError[IO,  Error] = new MonadError[IO, Error] {
  def pure[A](x: A): IO[A]               = IO.pure(x)
  def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
  def tailRecM[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] = 
    // check if the input is a Left or Right
    f(a).flatMap {
      case Left(nextA) => tailRecM(nextA)(f)
      case Right(b)    => IO.pure(b)
    }

  def raiseError[A](e: Error): IO[A]      = IO.raiseError(e) // Error must be Throwable

  override def handleErrorWith[A](fa: IO[A])(f: Error => IO[A]): IO[A] =
    fa.handleErrorWith {
      case e: Error => f(e)
      case other    => IO.raiseError(other)
    }

  // handleError: fallback to `handleErrorWith` + `pure`
  override def handleError[A](fa: IO[A])(f: Error => A): IO[A] =
    handleErrorWith(fa)(e => IO.pure(f(e)))
}


object NoOp extends ReadCommand[Unit]

class LogSpec extends CatsEffectSuite {

  // 1) An in‐memory LogStorage
  class InMemoryLogStorage[F[_]: Sync] extends LogStorage[F] {

    override def deleteBefore(index: Long): F[Unit] = ???

    private val ref = Ref.unsafe[F, Map[Long, LogEntry]](Map.empty)

    override def get(index: Long): F[Option[LogEntry]] =
      ref.get.map(_.get(index))

    override def getAtLength(length: Long): F[Option[LogEntry]] =
      get(length)

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


  // 2) A tiny TestLog that only wires in our in‐memory storage.
  //    Everything else (membershipManager, stateMachine, transactional, commitLog, etc.) is stubbed out.
  class TestLog extends Log[IO] {
    // real in‐memory storage
    override val logStorage = new InMemoryLogStorage[IO]
    // these are never called in our tests, so null is okay
    override val membershipManager = TestLog.defaultMembershipManager
    override val stateMachine      = new InMemoryStateMachine[IO]

    // identity transaction
    override def transactional[A](t: => IO[A]): IO[A] = t

    // commit‐index stored in a Ref so we can observe it if we wanted
    private val commitRef                           = Ref.unsafe[IO, Long](0L)
    override def getCommittedLength: IO[Long]       = commitRef.get
    override def setCommitLength(i: Long): IO[Unit] = commitRef.set(i)

    // Not used in these tests
    override def state: IO[LogState] = IO.pure(LogState(0, None, 0))

    // Stubs for the methods referenced by appendEntries (not needed here)
    def compactLogs(): IO[Unit]          = IO.unit
  }

  object TestLog {
    // a default 3‐node cluster with quorum=2
    val defaultMembershipManager = new MembershipManager[IO] {

      val configurationRef: Ref[IO, ClusterConfiguration] =
        Ref.unsafe[IO, ClusterConfiguration](
          ClusterConfiguration(
            currentNode = Leader(currentTerm = 1L, address = NodeAddress("n1", 9090)),
            members = List(NodeAddress("n1", 9090), NodeAddress("n2", 9090), NodeAddress("n3", 9090))
          )
        )

      override def members: IO[Set[Node]] = IO.pure(
        Set(
          Leader(currentTerm = 1L, address = NodeAddress("n1", 9090)),
          Follower(currentTerm = 1L, address = NodeAddress("n2", 9090)),
          Follower(currentTerm = 1L, address = NodeAddress("n3", 9090))
        )
      )

      override def setClusterConfiguration(newConfig: ClusterConfiguration): IO[Unit] = configurationRef.set(newConfig)

      def getClusterConfiguration: IO[ClusterConfiguration] = configurationRef.get
    }
  }



  // we need an implicit Logger[IO] for putEntries
  given logger: Logger[IO] = NoOpLogger[IO]

  var emptyDefer = new Deferred[IO, Unit] {

    val deffered = EffectDeferred.unsafe[IO, Unit]

    def get: IO[Unit] = deffered.get

    def complete(a: Unit) = deffered.complete(a)

  }

  test("truncateInconsistencyLog deletes all entries > leaderPrevLogLength when the last term mismatches") {
    for {
      log <- IO(new TestLog)
      store = log.logStorage
      // populate indices 1→term1, 2→term2, 3→term3
      _      <- store.put(1, LogEntry(1, 1, NoOp))
      _      <- store.put(2, LogEntry(2, 2, NoOp))
      _      <- store.put(3, LogEntry(3, 3, NoOp))
      before <- store.currentLength
      _ = assertEquals(before, 3L)

      // incoming head term = 2, but our last term = 3 → mismatch → deleteAfter(1)
      entries = List(LogEntry(2, 4, NoOp))
      _ <- log.truncateInconsistencyLog(entries, leaderPrevLogLength = 1L, currentLogLength = 3L)

      // only index 1 should remain
      e1 <- store.get(1)
      e2 <- store.get(2)
      e3 <- store.get(3)
    } yield {
      assert(e1.isDefined, "index 1 should remain")
      assert(e2.isEmpty, "index 2 should have been deleted")
      assert(e3.isEmpty, "index 3 should have been deleted")
    }
  }

  test("truncateInconsistencyLog is a no‐op when there is no mismatch or entries is empty") {
    for {
      log <- IO(new TestLog)
      store = log.logStorage
      _      <- store.put(1, LogEntry(5, 1, NoOp))
      before <- store.currentLength
      _ = assertEquals(before, 1L)

      // empty entries list → no deletion
      _ <- log.truncateInconsistencyLog(Nil, leaderPrevLogLength = 0L, currentLogLength = 1L)

      after <- store.currentLength
    } yield assertEquals(after, 1L)
  }

  test("putEntries appends all new entries when leaderPrevLogLength + entries.size > currentLength") {
    for {
      log <- IO(new TestLog)
      store = log.logStorage

      // pre‐populate index 1
      _    <- store.put(1, LogEntry(1, 1, NoOp))
      curr <- store.currentLength
      _ = assertEquals(curr, 1L)

      // two new entries (indices 2,3)
      newEntries = List(
        LogEntry(2, 2, NoOp),
        LogEntry(3, 3, NoOp)
      )
      // 1 + 2 > 1 → keep both
      _ <- log.putEntries(newEntries, leaderPrevLogLength = 1L, currentLogLength = 1L)

      e2 <- store.get(2)
      e3 <- store.get(3)
    } yield {
      assert(e2.isDefined, "index 2 should have been appended")
      assert(e3.isDefined, "index 3 should have been appended")
    }
  }

  test("putEntries does nothing when there are no new entries to append") {
    for {
      log <- IO(new TestLog)
      store = log.logStorage

      // pre‐populate two entries
      _    <- store.put(1, LogEntry(1, 1, NoOp))
      _    <- store.put(2, LogEntry(1, 2, NoOp))
      curr <- store.currentLength
      _ = assertEquals(curr, 2L)

      // leaderPrevLogLength + size(entries) = 2 + 0 <= 2 → drop all
      _ <- log.putEntries(Nil, leaderPrevLogLength = 2L, currentLogLength = 2L)

      after <- store.currentLength
    } yield assertEquals(after, 2L)
  }

  test("append should persist a LogEntry with the correct term and index") {
    for {
      log <- IO(new TestLog())
      // start with an empty log; currentLength == 0
      beforeLen <- log.logStorage.currentLength
      _ = assertEquals(beforeLen, 0L)

      // append a NoOp command at term=42
      entry    <- log.append(42L, NoOp, emptyDefer)(using ioMonadErrorForError, logger)

      afterLen <- log.logStorage.currentLength

      stored <- log.logStorage.get(entry.index)
    } yield {
      // we expect the first entry to be index=1, term=42
      assertEquals(entry.index, 1L)
      assertEquals(entry.term, 42L)

      // the store should also contain it
      assertEquals(afterLen, 1L)
      assert(stored.isDefined)
      assertEquals(stored.get.term, entry.term)
      assertEquals(stored.get.index, entry.index)
    }
  }

  // 2) Test for commitLogs(...)
  test("commitLogs should advance commitIndex when ackLengthMap reaches quorum") {
    // build a specialized TestLog whose membershipManager has quorum=2

    for {
      log <- IO(new TestLog)
      store = log.logStorage

      // populate two entries
      _ <- store.put(0, LogEntry(1, 0, NoOp))
      _ <- store.put(1, LogEntry(1, 1, NoOp))
      _ <- log.setCommitLength(1L) // initial commitIndex = 1

      // map of acknowledgments from 3 nodes:
      //  - "a" and "b" have seen up to index=2 (>=2)
      //  - "c" only up to index=1
      acks = Map(
        NodeAddress("a", 9090) -> 2L,
        NodeAddress("b", 9090) -> 2L,
        NodeAddress("c", 9090) -> 1L
      )

      // invoke commitLogs
      result    <- log.commitLogs(acks)
      newCommit <- log.getCommittedLength
    } yield {
      // because two nodes (a,b) acked index=2, which meets quorum=2,
      // commitLogs should return true and the commitIndex should advance to 3 (2+1)
      assertEquals(result, true)
      assertEquals(newCommit, 2L)
    }
  }

}
