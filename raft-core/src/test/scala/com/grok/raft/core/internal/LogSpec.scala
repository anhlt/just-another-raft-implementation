package com.grok.raft.core.internal

import cats.effect._
import cats.effect.kernel.Ref
import munit.CatsEffectSuite
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

// Bring your domain types into scope
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core._


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

  // 2) A tiny TestLog that only wires in our in‐memory storage.
  //    Everything else (membershipManager, stateMachine, transactional, commitLog, etc.) is stubbed out.
  class TestLog extends Log[IO] {
    // real in‐memory storage
    override val logStorage               = new InMemoryLogStorage[IO]
    // these are never called in our tests, so null is okay
    override val membershipManager        = null
    override val stateMachine             = null

    // identity transaction
    override def transactional[A](t: => IO[A]): IO[A] = t

    // commit‐index stored in a Ref so we can observe it if we wanted
    private val commitRef = Ref.unsafe[IO, Long](0L)
    override def getCommittedLength: IO[Long] = commitRef.get
    override def setCommitLength(i: Long): IO[Unit] = commitRef.set(i)

    // Not used in these tests
    override def state: IO[LogState] = IO.pure(LogState(0, None, 0))

    // Stubs for the methods referenced by appendEntries (not needed here)
    def commitLog(index: Long): IO[Long] = setCommitLength(index).as(index)
    def compactLogs(): IO[Unit]       = IO.unit
  }

  // we need an implicit Logger[IO] for putEntries
  implicit val logger: Logger[IO] = NoOpLogger[IO]

  test("truncateInconsistencyLog deletes all entries > leaderPrevLogLength when the last term mismatches") {
    for {
      log     <- IO(new TestLog)
      store    = log.logStorage
      // populate indices 1→term1, 2→term2, 3→term3
      _       <- store.put(1, LogEntry(1, 1, NoOp))
      _       <- store.put(2, LogEntry(2, 2, NoOp))
      _       <- store.put(3, LogEntry(3, 3, NoOp))
      before  <- store.currentLength
      _        = assertEquals(before, 3L)

      // incoming head term = 2, but our last term = 3 → mismatch → deleteAfter(1)
      entries  = List(LogEntry(2, 4, NoOp))
      _       <- log.truncateInconsistencyLog(entries, leaderPrevLogLength = 1L, currentLogLength = 3L)

      // only index 1 should remain
      e1      <- store.get(1)
      e2      <- store.get(2)
      e3      <- store.get(3)
    } yield {
      assert(e1.isDefined, "index 1 should remain")
      assert(e2.isEmpty,   "index 2 should have been deleted")
      assert(e3.isEmpty,   "index 3 should have been deleted")
    }
  }

  test("truncateInconsistencyLog is a no‐op when there is no mismatch or entries is empty") {
    for {
      log     <- IO(new TestLog)
      store    = log.logStorage
      _       <- store.put(1, LogEntry(5, 1, NoOp))
      before  <- store.currentLength
      _        = assertEquals(before, 1L)

      // empty entries list → no deletion
      _       <- log.truncateInconsistencyLog(Nil, leaderPrevLogLength = 0L, currentLogLength = 1L)

      after   <- store.currentLength
    } yield assertEquals(after, 1L)
  }

  test("putEntries appends all new entries when leaderPrevLogLength + entries.size > currentLength") {
    for {
      log      <- IO(new TestLog)
      store     = log.logStorage

      // pre‐populate index 1
      _        <- store.put(1, LogEntry(1, 1, NoOp))
      curr     <- store.currentLength
      _         = assertEquals(curr, 1L)

      // two new entries (indices 2,3)
      newEntries = List(
        LogEntry(2, 2, NoOp),
        LogEntry(3, 3, NoOp)
      )
      // 1 + 2 > 1 → keep both
      _        <- log.putEntries(newEntries, leaderPrevLogLength = 1L, currentLogLength = 1L)

      e2       <- store.get(2)
      e3       <- store.get(3)
    } yield {
      assert(e2.isDefined, "index 2 should have been appended")
      assert(e3.isDefined, "index 3 should have been appended")
    }
  }

  test("putEntries does nothing when there are no new entries to append") {
    for {
      log      <- IO(new TestLog)
      store     = log.logStorage

      // pre‐populate two entries
      _        <- store.put(1, LogEntry(1, 1, NoOp))
      _        <- store.put(2, LogEntry(1, 2, NoOp))
      curr     <- store.currentLength
      _         = assertEquals(curr, 2L)

      // leaderPrevLogLength + size(entries) = 2 + 0 <= 2 → drop all
      _        <- log.putEntries(Nil, leaderPrevLogLength = 2L, currentLogLength = 2L)

      after    <- store.currentLength
    } yield assertEquals(after, 2L)
  }
}