package com.grok.raft.core.internal

import cats.*
import cats.effect.*
import cats.effect.kernel.{Deferred => EffectDeferred}
import com.grok.raft.core.*
import munit.CatsEffectSuite

class LogStateIndexConsistencySpec extends CatsEffectSuite {

  // Import MTL test utilities
  import MtlTestUtils.*
  import MtlTestUtils.given

  var emptyDefer = new com.grok.raft.core.internal.RaftDeferred[IO, Unit] {
    val deffered          = EffectDeferred.unsafe[IO, Unit]
    def get: IO[Unit]     = deffered.get
    def complete(a: Unit) = deffered.complete(a)
  }

  test("LogState.lastLogIndex should correctly convert length to 0-based index") {
    val emptyLog = LogState(lastLogIndex = -1L, lastLogTerm = None)
    assertEquals(emptyLog.lastLogIndex, -1L)

    val singleEntry = LogState(lastLogIndex = 0L, lastLogTerm = Some(1L))
    assertEquals(singleEntry.lastLogIndex, 0L)

    val multipleEntries = LogState(lastLogIndex = 9L, lastLogTerm = Some(3L))
    assertEquals(multipleEntries.lastLogIndex, 9L)

    val largeLog = LogState(lastLogIndex = 999L, lastLogTerm = Some(42L))
    assertEquals(largeLog.lastLogIndex, 999L)
  }

  test("LogState should handle edge cases consistently") {
    // Test with maximum long value (theoretical edge case)
    val maxLog = LogState(lastLogIndex = Long.MaxValue - 1, lastLogTerm = Some(1L))
    assertEquals(maxLog.lastLogIndex, Long.MaxValue - 1)

    // Test with appliedLogIndex tracking
    val logWithApplied = LogState(lastLogIndex = 4L, lastLogTerm = Some(2L), appliedLogIndex = 3L)
    assertEquals(logWithApplied.lastLogIndex, 4L)
    assertEquals(logWithApplied.appliedLogIndex, 3L)
    assert(logWithApplied.appliedLogIndex < logWithApplied.lastLogIndex) // applied should be <= last
  }

  test("Log.commitLogs should advance commit index based on quorum acknowledgments") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(2, 2, NoOp))
        _ <- store.put(3, LogEntry(2, 3, NoOp))

        // Set initial commit index
        _ <- log.setCommitIndex(-1L)

        // Acknowledgments: majority (2/3) acked up to index 1, one up to index 3
        acks = Map(
          NodeAddress("a", 9090) -> 1L,
          NodeAddress("b", 9090) -> 1L,
          NodeAddress("c", 9090) -> 3L
        )

        result         <- log.commitLogs(acks)
        newCommitIndex <- log.getCommittedIndex
      } yield {
        assertEquals(result, true)
        assertEquals(newCommitIndex, 1L) // Should advance to highest quorum-acked index
      }
    }
  }

  test("Log.commitLogs should handle no majority case") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(2, 2, NoOp))

        _ <- log.setCommitIndex(-1L)

        // No majority scenario: only 1 out of 3 nodes acked anything
        // (2 nodes have acked nothing, 1 node acked up to index 1)
        acks = Map(
          NodeAddress("a", 9090) -> 1L,
          NodeAddress("b", 9090) -> -1L, // no acks
          NodeAddress("c", 9090) -> -1L  // no acks
        )

        result         <- log.commitLogs(acks)
        newCommitIndex <- log.getCommittedIndex
      } yield {
        assertEquals(result, false)       // returns false because no entries were committed
        assertEquals(newCommitIndex, -1L) // Should not advance commit index
      }
    }
  }

  test("Log.commitLogs should handle progressive commit advancement") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(2, 2, NoOp))
        _ <- store.put(3, LogEntry(2, 3, NoOp))

        _ <- log.setCommitIndex(-1L)

        // First round: majority acked up to index 1
        acks1 = Map(
          NodeAddress("a", 9090) -> 1L,
          NodeAddress("b", 9090) -> 1L,
          NodeAddress("c", 9090) -> 0L
        )

        result1      <- log.commitLogs(acks1)
        commitIndex1 <- log.getCommittedIndex

        // Second round: majority now acked up to index 3
        acks2 = Map(
          NodeAddress("a", 9090) -> 3L,
          NodeAddress("b", 9090) -> 3L,
          NodeAddress("c", 9090) -> 1L
        )

        result2      <- log.commitLogs(acks2)
        commitIndex2 <- log.getCommittedIndex
      } yield {
        assertEquals(result1, true)
        assertEquals(commitIndex1, 1L)
        assertEquals(result2, true)
        assertEquals(commitIndex2, 3L) // Should advance from 1 to 3
      }
    }
  }

  test("Log.append should correctly calculate next index") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Start with empty log
        initialIndex <- store.lastIndex
        _ = assertEquals(initialIndex, -1L)

        // Append first entry
        entry1 <- log.append(1L, NoOp, emptyDefer)
        _ = assertEquals(entry1.index, 0L)
        _ = assertEquals(entry1.term, 1L)

        // Append second entry
        entry2 <- log.append(1L, NoOp, emptyDefer)
        _ = assertEquals(entry2.index, 1L)
        _ = assertEquals(entry2.term, 1L)

        // Append entry with different term
        entry3 <- log.append(2L, NoOp, emptyDefer)
        _ = assertEquals(entry3.index, 2L)
        _ = assertEquals(entry3.term, 2L)

        finalIndex <- store.lastIndex
      } yield assertEquals(finalIndex, 2L)
    }
  }

  test("Log.putEntries should handle index calculations correctly") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with some entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))

        currentLogIndex <- store.lastIndex
        _ = assertEquals(currentLogIndex, 1L)

        // New entries starting from index 2
        newEntries = List(
          LogEntry(2, 2, NoOp),
          LogEntry(2, 3, NoOp),
          LogEntry(2, 4, NoOp)
        )

        // leaderPrevLogIndex + entries.size = 1 + 3 = 4 > currentLogIndex (1)
        // So all entries should be appended
        _ <- log.putEntries(newEntries, leaderPrevLogIndex = 1L, currentLogIndex = 1L)

        // Check all entries were stored
        entry2     <- store.get(2)
        entry3     <- store.get(3)
        entry4     <- store.get(4)
        finalIndex <- store.lastIndex
      } yield {
        assert(entry2.isDefined)
        assert(entry3.isDefined)
        assert(entry4.isDefined)
        assertEquals(finalIndex, 4L)
      }
    }
  }

  test("Log.putEntries should drop entries when they would duplicate existing ones") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with entries up to index 4
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(1, 2, NoOp))
        _ <- store.put(3, LogEntry(1, 3, NoOp))
        _ <- store.put(4, LogEntry(1, 4, NoOp))

        currentLogIndex <- store.lastIndex
        _ = assertEquals(currentLogIndex, 4L)

        // Entries that would overlap with existing ones
        overlappingEntries = List(
          LogEntry(2, 3, NoOp), // would go to index 3 (already exists)
          LogEntry(2, 4, NoOp), // would go to index 4 (already exists)
          LogEntry(2, 5, NoOp)  // would go to index 5 (new)
        )

        // leaderPrevLogIndex + entries.size = 2 + 3 = 5 > currentLogIndex (4)
        // But some entries should be dropped due to overlap calculation
        _ <- log.putEntries(overlappingEntries, leaderPrevLogIndex = 2L, currentLogIndex = 4L)

        // Check that only the new entry was added
        entry5     <- store.get(5)
        finalIndex <- store.lastIndex
      } yield {
        assert(entry5.isDefined)
        assertEquals(entry5.get.index, 5L)
        assertEquals(finalIndex, 5L)
      }
    }
  }

  test("Log.putEntries should handle empty entries list") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate with some entries
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))

        initialIndex <- store.lastIndex
        _ = assertEquals(initialIndex, 1L)

        // Try to put empty entries list
        _ <- log.putEntries(Nil, leaderPrevLogIndex = 1L, currentLogIndex = 1L)

        finalIndex <- store.lastIndex
      } yield assertEquals(finalIndex, 1L) // Should remain unchanged
    }
  }

  test("Log.truncateInconsistencyLog should handle term mismatch correctly") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate: indices 0→term1, 1→term1, 2→term2
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(2, 2, NoOp))

        beforeIndex <- store.lastIndex
        _ = assertEquals(beforeIndex, 2L)

        // Incoming entries with term mismatch at position after leaderPrevLogIndex
        entries = List(LogEntry(3, 2, NoOp)) // term 3, but existing entry at index 2 has term 2

        _ <- log.truncateInconsistencyLog(entries, leaderPrevLogIndex = 1L, currentLogIndex = 2L)

        // Should have deleted entry at index 2 and beyond
        entry0     <- store.get(0)
        entry1     <- store.get(1)
        entry2     <- store.get(2)
        afterIndex <- store.lastIndex
      } yield {
        assert(entry0.isDefined)
        assert(entry1.isDefined)
        assert(entry2.isEmpty) // Should be deleted due to term mismatch
        assertEquals(afterIndex, 1L)
      }
    }
  }

  test("Log.truncateInconsistencyLog should be no-op when terms match") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        // Pre-populate
        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))
        _ <- store.put(2, LogEntry(2, 2, NoOp))

        // Incoming entries with matching term
        entries = List(LogEntry(2, 3, NoOp)) // term 2 matches existing entry at index 2

        _ <- log.truncateInconsistencyLog(entries, leaderPrevLogIndex = 1L, currentLogIndex = 2L)

        // All entries should remain
        entry0     <- store.get(0)
        entry1     <- store.get(1)
        entry2     <- store.get(2)
        afterIndex <- store.lastIndex
      } yield {
        assert(entry0.isDefined)
        assert(entry1.isDefined)
        assert(entry2.isDefined) // Should remain
        assertEquals(afterIndex, 2L)
      }
    }
  }

  test("Log.truncateInconsistencyLog should be no-op with empty entries") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))

        beforeIndex <- store.lastIndex

        _ <- log.truncateInconsistencyLog(Nil, leaderPrevLogIndex = 0L, currentLogIndex = 1L)

        afterIndex <- store.lastIndex
      } yield assertEquals(afterIndex, beforeIndex) // Should be unchanged
    }
  }

  test("Log.truncateInconsistencyLog should be no-op when currentLogIndex <= leaderPrevLogIndex") {
    withLogErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, Unit])
        store = log.logStorage

        _ <- store.put(0, LogEntry(1, 0, NoOp))
        _ <- store.put(1, LogEntry(1, 1, NoOp))

        beforeIndex <- store.lastIndex

        entries = List(LogEntry(2, 2, NoOp))
        // currentLogIndex (1) <= leaderPrevLogIndex (2)
        _ <- log.truncateInconsistencyLog(entries, leaderPrevLogIndex = 2L, currentLogIndex = 1L)

        afterIndex <- store.lastIndex
      } yield assertEquals(afterIndex, beforeIndex) // Should be unchanged
    }
  }
}
