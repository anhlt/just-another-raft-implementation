package com.grok.raft.core.internal

import cats.*
import cats.effect.*
import cats.effect.kernel.{Deferred => EffectDeferred}
import com.grok.raft.core.*
import com.grok.raft.core.storage.Snapshot
import munit.CatsEffectSuite


class LogSpec extends CatsEffectSuite {

  // Import MTL test utilities
  import MtlTestUtils.*
  import MtlTestUtils.given


  var emptyDefer = new com.grok.raft.core.internal.RaftDeferred[IO, Unit] {

    val deffered = EffectDeferred.unsafe[IO, Unit]

    def get: IO[Unit] = deffered.get

    def complete(a: Unit) = deffered.complete(a)

  }

  test("truncateInconsistencyLog deletes all entries > leaderPrevLogIndex when the last term mismatches") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        store = log.logStorage
        // populate indices 0→term0, 1→term1, 2→term2
        _      <- store.put(0, LogEntry(0, 0, NoOp))
        _      <- store.put(1, LogEntry(1, 1, NoOp))
        _      <- store.put(2, LogEntry(2, 2, NoOp))
        before <- store.lastIndex
        _ = assertEquals(before, 2L) // last index is 2 (for 3 entries: 0,1,2)

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
  }

  test("truncateInconsistencyLog is a no‐op when there is no mismatch or entries is empty") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        store = log.logStorage
        _      <- store.put(1, LogEntry(5, 1, NoOp))
        before <- store.lastIndex
        _ = assertEquals(before, 1L) // last index is 1 (we put entry at index 1)

        // empty entries list → no deletion
        _ <- log.truncateInconsistencyLog(Nil, leaderPrevLogIndex = 0L, currentLogIndex = 0L)

        after <- store.lastIndex
      } yield assertEquals(after, 1L) // still index 1 after no-op
    }
  }

  test("putEntries appends all new entries when leaderPrevLogIndex + entries.size > currentLogIndex") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        store = log.logStorage

        // pre‐populate index 1
        _    <- store.put(1, LogEntry(1, 1, NoOp))
        curr <- store.lastIndex
        _ = assertEquals(curr, 1L) // last index is 1 (we put entry at index 1)

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
  }

  test("putEntries does nothing when there are no new entries to append") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        store = log.logStorage

        // pre‐populate two entries
        _    <- store.put(1, LogEntry(1, 1, NoOp))
        _    <- store.put(2, LogEntry(1, 2, NoOp))
        curr <- store.lastIndex
        _ = assertEquals(curr, 2L) // last index is 2 (we have entries at indices 1,2)

        // leaderPrevLogIndex + size(entries) = 1 + 0 <= 1 → drop all
        _ <- log.putEntries(Nil, leaderPrevLogIndex = 1L, currentLogIndex = 1L)

        after <- store.lastIndex
      } yield assertEquals(after, 2L) // still index 2 after no-op
    }
  }

  test("append should persist a LogEntry with the correct term and index") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        // start with an empty log; lastIndex == -1
        beforeLen <- log.logStorage.lastIndex
        _ = assertEquals(beforeLen, -1L)

        // append a NoOp command at term=42
        entry    <- log.append(42L, NoOp, emptyDefer)
        afterLen <- log.logStorage.lastIndex

        stored <- log.logStorage.get(entry.index)
      } yield {
        // we expect the first entry to be index=0, term=42
        assertEquals(entry.index, 0L)
        assertEquals(entry.term, 42L)

        // the store should also contain it
        assertEquals(afterLen, 0L) // last index is 0 (first entry)
        assert(stored.isDefined)
        assertEquals(stored.get.term, entry.term)
        assertEquals(stored.get.index, entry.index)
      }
    }
  }

  // 2) Test for commitLogs(...)
  test("commitLogs should advance commitIndex when ackIndexMap reaches quorum") {
    // build a specialized TestLog whose membershipManager has quorum=2

    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        store = log.logStorage

        // populate two entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- log.setCommitIndex(0L) // initial commitIndex = 0

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
        newCommit <- log.getCommittedIndex
      } yield {
        // because two nodes (a,b) acked index=1, which meets quorum=2,
        // commitLogs should return true and the commitIndex should advance to 1
        assertEquals(result, true)
        assertEquals(newCommit, 1L)
      }
    }
  }

  // Snapshot tests
  test("createSnapshot should capture current state and persist it") {
    withStateMachineErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        _ <- log.stateMachine.restoreSnapshot(5L, "test-state")
        config <- log.membershipManager.getClusterConfiguration
        
        snapshot <- log.createSnapshot(5L)
        retrieved <- log.snapshotStorage.retrieveSnapshot
      } yield {
        assertEquals(snapshot.lastIndex, 5L)
        assertEquals(snapshot.data, "test-state")
        assertEquals(snapshot.config, config)
        assert(retrieved.isDefined)
        assertEquals(retrieved.get, snapshot)
      }
    }
  }

  test("installSnapshot should restore state and truncate log") {
    withLogErrorHandling {
      withStateMachineErrorHandling {
        for {
          log <- IO(new InMemoryLog[IO, String])
          store = log.logStorage
          
          // populate some log entries
          _ <- store.put(1, LogEntry(1, 1, NoOp))
          _ <- store.put(2, LogEntry(1, 2, NoOp))
          _ <- store.put(3, LogEntry(1, 3, NoOp))
          
          config <- log.membershipManager.getClusterConfiguration
          snapshot = Snapshot(2L, "snapshot-state", config)
          
          _ <- log.installSnapshot(snapshot)
          
          // Check state was restored
          state <- log.stateMachine.getCurrentState
          appliedIndex <- log.stateMachine.appliedIndex
          commitIndex <- log.getCommittedIndex
          
          // Check log was truncated
          e1 <- store.get(1)
          e2 <- store.get(2)
          e3 <- store.get(3)
        } yield {
          assertEquals(state, "snapshot-state")
          assertEquals(appliedIndex, 2L)
          assertEquals(commitIndex, 2L)
          assert(e1.isDefined, "entry 1 should remain")
          assert(e2.isDefined, "entry 2 should remain")
          assert(e3.isEmpty, "entry 3 should be deleted")
        }
      }
    }
  }

  test("shouldCreateSnapshot should return true when logs exceed threshold") {
    withStateMachineErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        
        // Set applied index to simulate many logs since last snapshot
        _ <- log.stateMachine.restoreSnapshot(1500L, "state")
        
        shouldCreate <- log.shouldCreateSnapshot()
      } yield {
        assertEquals(shouldCreate, true)
      }
    }
  }

  test("shouldCreateSnapshot should return false when logs are under threshold") {
    withStateMachineErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        
        // Set applied index to simulate few logs since last snapshot
        _ <- log.stateMachine.restoreSnapshot(100L, "state")
        
        shouldCreate <- log.shouldCreateSnapshot()
      } yield {
        assertEquals(shouldCreate, false)
      }
    }
  }

  test("getSnapshotMetadata should return latest snapshot info") {
    for {
      log <- IO(new InMemoryLog[IO, String])
      config <- log.membershipManager.getClusterConfiguration
      snapshot = Snapshot(10L, "meta-state", config)
      
      _ <- log.snapshotStorage.persistSnapshot(snapshot)
      metadata <- log.getSnapshotMetadata()
    } yield {
      assert(metadata.isDefined)
      assertEquals(metadata.get._1, 10L)
      assertEquals(metadata.get._2, config)
    }
  }

  test("compactLogs should create snapshot and delete old entries when threshold is met") {
    withLogErrorHandling {
      withStateMachineErrorHandling {
        for {
          log <- IO(new InMemoryLog[IO, String])
          store = log.logStorage
          
          // Set up state to trigger compaction
          _ <- log.stateMachine.restoreSnapshot(1500L, "compact-state")
          
          // Add some log entries
          _ <- store.put(1498, LogEntry(1, 1498, NoOp))
          _ <- store.put(1499, LogEntry(1, 1499, NoOp))
          _ <- store.put(1500, LogEntry(1, 1500, NoOp))
          
          _ <- log.compactLogs()
          
          // Check snapshot was created
          snapshot <- log.snapshotStorage.retrieveSnapshot
          
          // Check entries before applied index were deleted
          e1498 <- store.get(1498)
          e1500 <- store.get(1500)
        } yield {
          assert(snapshot.isDefined)
          assertEquals(snapshot.get.lastIndex, 1500L)
          assert(e1498.isEmpty, "entry 1498 should be deleted")
          assert(e1500.isDefined, "entry 1500 should remain")
        }
      }
    }
  }

}
