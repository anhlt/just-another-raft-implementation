package com.grok.raft.core.internal

import cats.*
import cats.effect.*
import cats.effect.kernel.{Deferred => EffectDeferred}
import com.grok.raft.core.*
import munit.CatsEffectSuite


class LogSpec extends CatsEffectSuite {


  var emptyDefer = new com.grok.raft.core.internal.RaftDeferred[IO, Unit] {

    val deffered = EffectDeferred.unsafe[IO, Unit]

    def get: IO[Unit] = deffered.get

    def complete(a: Unit) = deffered.complete(a)

  }

  test("truncateInconsistencyLog deletes all entries > leaderPrevLogIndex when the last term mismatches") {
    for {
      log <- IO(new InMemoryLog[IO])
      store = log.logStorage
      // populate indices 0→term0, 1→term1, 2→term2
      _      <- store.put(0, LogEntry(0, 0, NoOp))
      _      <- store.put(1, LogEntry(1, 1, NoOp))
      _      <- store.put(2, LogEntry(2, 2, NoOp))
      before <- store.currentLength
      _ = assertEquals(before, 3L)

      // incoming head term = 1, but our last term = 2 → mismatch → deleteAfter(1)
      entries = List(LogEntry(1, 3, NoOp))
      _ <- log.truncateInconsistencyLog(entries, leaderPrevLogIndex = 1L, currentLogIndex = 2L)

      // indices 0 and 1 should remain, index 2 should be deleted
      e0 <- store.get(0)
      e1 <- store.get(1)
      e2 <- store.get(2)
    } yield {
      assert(e0.isDefined, "index 0 should remain")
      assert(e1.isDefined, "index 1 should remain")
      assert(e2.isEmpty, "index 2 should have been deleted")
    }
  }

  test("truncateInconsistencyLog is a no‐op when there is no mismatch or entries is empty") {
    for {
      log <- IO(new InMemoryLog[IO])
      store = log.logStorage
      _      <- store.put(1, LogEntry(5, 1, NoOp))
      before <- store.currentLength
      _ = assertEquals(before, 1L)

      // empty entries list → no deletion
      _ <- log.truncateInconsistencyLog(Nil, leaderPrevLogIndex = 0L, currentLogIndex = 0L)

      after <- store.currentLength
    } yield assertEquals(after, 1L)
  }

  test("putEntries appends all new entries when leaderPrevLogIndex + entries.size > currentLogIndex") {
    for {
      log <- IO(new InMemoryLog[IO])
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
      // 1 + 2 > 0 → keep both  
      _ <- log.putEntries(newEntries, leaderPrevLogIndex = 1L, currentLogIndex = 0L)

      e2 <- store.get(2)
      e3 <- store.get(3)
    } yield {
      assert(e2.isDefined, "index 2 should have been appended")
      assert(e3.isDefined, "index 3 should have been appended")
    }
  }

  test("putEntries does nothing when there are no new entries to append") {
    for {
      log <- IO(new InMemoryLog[IO])
      store = log.logStorage

      // pre‐populate two entries
      _    <- store.put(1, LogEntry(1, 1, NoOp))
      _    <- store.put(2, LogEntry(1, 2, NoOp))
      curr <- store.currentLength
      _ = assertEquals(curr, 2L)

      // leaderPrevLogIndex + size(entries) = 1 + 0 <= 1 → drop all
      _ <- log.putEntries(Nil, leaderPrevLogIndex = 1L, currentLogIndex = 1L)

      after <- store.currentLength
    } yield assertEquals(after, 2L)
  }

  test("append should persist a LogEntry with the correct term and index") {
    for {
      log <- IO(new InMemoryLog[IO])
      // start with an empty log; currentLength == 0
      beforeLen <- log.logStorage.currentLength
      _ = assertEquals(beforeLen, 0L)

      // append a NoOp command at term=42
      entry    <- log.append(42L, NoOp, emptyDefer)
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
  test("commitLogs should advance commitIndex when ackIndexMap reaches quorum") {
    // build a specialized TestLog whose membershipManager has quorum=2

    for {
      log <- IO(new InMemoryLog[IO])
      store = log.logStorage

      // populate two entries
      _ <- store.put(0, LogEntry(1, 0, NoOp))
      _ <- store.put(1, LogEntry(1, 1, NoOp))
      _ <- log.setCommitLength(1L) // initial commitIndex = 1

      // map of acknowledgments from 3 nodes:
      //  - "a" and "b" have seen up to index=1 (>=1)
      //  - "c" only up to index=0
      acks = Map(
        NodeAddress("a", 9090) -> 1L,
        NodeAddress("b", 9090) -> 1L,
        NodeAddress("c", 9090) -> 0L
      )

      // invoke commitLogs
      result    <- log.commitLogs(acks)
      newCommit <- log.getCommittedLength
    } yield {
      // because two nodes (a,b) acked index=1, which meets quorum=2,
      // commitLogs should return true and the commitIndex should advance to 2 (1+1)
      assertEquals(result, true)
      assertEquals(newCommit, 2L)
    }
  }

}
