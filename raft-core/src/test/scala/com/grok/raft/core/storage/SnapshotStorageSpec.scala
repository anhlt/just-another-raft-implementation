package com.grok.raft.core.storage

import cats.*
import cats.effect.*
import com.grok.raft.core.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.internal.given
import com.grok.raft.core.storage.Snapshot
import munit.CatsEffectSuite

class SnapshotStorageSpec extends CatsEffectSuite {

  import MtlTestUtils.*
  import MtlTestUtils.given
  import TestData.*

  val testConfig = ClusterConfiguration(
    currentNode = leader,
    members = List(addr1, addr2, addr3)
  )

  test("persistSnapshot should store snapshot successfully") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      snapshot = Snapshot(42L, "test-state", testConfig)

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.retrieveSnapshot
    } yield {
      assert(retrieved.isDefined)
      assertEquals(retrieved.get.lastIndex, 42L)
      assertEquals(retrieved.get.data, "test-state")
      assertEquals(retrieved.get.config, testConfig)
    }
  }

  test("retrieveSnapshot should return None when no snapshot exists") {
    for {
      storage   <- IO(new InMemorySnapshotStorage[IO, String])
      retrieved <- storage.retrieveSnapshot
    } yield {
      assert(retrieved.isEmpty)
    }
  }

  test("getLatestSnapshot should throw when no snapshot exists") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      result  <- storage.getLatestSnapshot.attempt
    } yield {
      assert(result.isLeft)
    }
  }

  test("getLatestSnapshot should return snapshot when it exists") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      snapshot = Snapshot(100L, "latest-state", testConfig)

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.getLatestSnapshot
    } yield {
      assertEquals(retrieved.lastIndex, 100L)
      assertEquals(retrieved.data, "latest-state")
      assertEquals(retrieved.config, testConfig)
    }
  }

  test("persistSnapshot should overwrite previous snapshot") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      snapshot1 = Snapshot(10L, "first-state", testConfig)
      snapshot2 = Snapshot(20L, "second-state", testConfig)

      _     <- storage.persistSnapshot(snapshot1)
      first <- storage.retrieveSnapshot

      _      <- storage.persistSnapshot(snapshot2)
      second <- storage.retrieveSnapshot
    } yield {
      assertEquals(first.get.lastIndex, 10L)
      assertEquals(first.get.data, "first-state")

      assertEquals(second.get.lastIndex, 20L)
      assertEquals(second.get.data, "second-state")
    }
  }

  test("multiple snapshots with different data types should work") {
    for {
      stringStorage <- IO(new InMemorySnapshotStorage[IO, String])
      intStorage    <- IO(new InMemorySnapshotStorage[IO, Int])

      stringSnapshot = Snapshot(5L, "string-data", testConfig)
      intSnapshot    = Snapshot(10L, 42, testConfig)

      _ <- stringStorage.persistSnapshot(stringSnapshot)
      _ <- intStorage.persistSnapshot(intSnapshot)

      retrievedString <- stringStorage.retrieveSnapshot
      retrievedInt    <- intStorage.retrieveSnapshot
    } yield {
      assertEquals(retrievedString.get.data, "string-data")
      assertEquals(retrievedInt.get.data, 42)
    }
  }

  test("snapshot data integrity should be preserved") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, Map[String, Int]])
      complexData = Map("key1" -> 100, "key2" -> 200, "key3" -> 300)
      snapshot    = Snapshot(15L, complexData, testConfig)

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.retrieveSnapshot
    } yield {
      assertEquals(retrieved.get.data, complexData)
      assertEquals(retrieved.get.data("key1"), 100)
      assertEquals(retrieved.get.data("key2"), 200)
      assertEquals(retrieved.get.data("key3"), 300)
    }
  }

  test("createSnapshot should create and persist snapshot with correct data") {
    withStateMachineErrorHandling {
      for {
        log    <- IO(new InMemoryLog[IO, String])
        _      <- log.stateMachine.restoreSnapshot(25L, "snapshot-test-state")
        config <- log.membershipManager.getClusterConfiguration

        _         <- log.createSnapshot(25L)
        snapshot  <- log.createSnapshot(25L)
        retrieved <- log.snapshotStorage.retrieveSnapshot
      } yield {
        assertEquals(snapshot.lastIndex, 25L)
        assertEquals(snapshot.data, "snapshot-test-state")
        assertEquals(snapshot.config, config)

        assert(retrieved.isDefined)
        assertEquals(retrieved.get, snapshot)
      }
    }
  }

  test("createSnapshot should capture current state machine state") {
    withStateMachineErrorHandling {
      for {
        log <- IO(new InMemoryLog[IO, String])
        initialState = "initial-state"
        updatedState = "updated-state"

        _         <- log.stateMachine.restoreSnapshot(10L, initialState)
        snapshot1 <- log.createSnapshot(10L)

        _         <- log.stateMachine.restoreSnapshot(20L, updatedState)
        snapshot2 <- log.createSnapshot(20L)
      } yield {
        assertEquals(snapshot1.data, initialState)
        assertEquals(snapshot1.lastIndex, 10L)

        assertEquals(snapshot2.data, updatedState)
        assertEquals(snapshot2.lastIndex, 20L)
      }
    }
  }

  test("restoreSnapshot should restore state machine to snapshot state") {
    withLogErrorHandling {
      withStateMachineErrorHandling {
        for {
          log    <- IO(new InMemoryLog[IO, String])
          config <- log.membershipManager.getClusterConfiguration
          snapshot = Snapshot(50L, "restored-state", config)

          _ <- log.restoreSnapshot(snapshot)

          state        <- log.stateMachine.getCurrentState
          appliedIndex <- log.stateMachine.appliedIndex
          commitIndex  <- log.getCommittedIndex
        } yield {
          assertEquals(state, "restored-state")
          assertEquals(appliedIndex, 50L)
          assertEquals(commitIndex, 50L)
        }
      }
    }
  }

  test("installSnapshot should persist and restore snapshot") {
    withLogErrorHandling {
      withStateMachineErrorHandling {
        for {
          log <- IO(new InMemoryLog[IO, String])
          store = log.logStorage

          // Add some log entries
          _ <- store.put(45, LogEntry(1, 45, NoOp))
          _ <- store.put(46, LogEntry(1, 46, NoOp))
          _ <- store.put(47, LogEntry(1, 47, NoOp))
          _ <- store.put(48, LogEntry(1, 48, NoOp))
          _ <- store.put(49, LogEntry(1, 49, NoOp))
          _ <- store.put(50, LogEntry(1, 50, NoOp))

          config <- log.membershipManager.getClusterConfiguration
          snapshot = Snapshot(47L, "install-test-state", config)

          _ <- log.installSnapshot(snapshot)

          // Verify snapshot was persisted
          retrieved <- log.snapshotStorage.retrieveSnapshot

          // Verify state was restored
          state        <- log.stateMachine.getCurrentState
          appliedIndex <- log.stateMachine.appliedIndex
          commitIndex  <- log.getCommittedIndex

          // Verify log was truncated correctly (entries after snapshot index should be gone)
          e45 <- store.get(45)
          e46 <- store.get(46)
          e47 <- store.get(47)
          e48 <- store.get(48)
          e49 <- store.get(49)
          e50 <- store.get(50)
        } yield {
          // Snapshot should be persisted
          assert(retrieved.isDefined)
          assertEquals(retrieved.get, snapshot)

          // State should be restored
          assertEquals(state, "install-test-state")
          assertEquals(appliedIndex, 47L)
          assertEquals(commitIndex, 47L)

          // Entries up to and including snapshot index should remain
          assert(e45.isDefined, "entry 45 should remain")
          assert(e46.isDefined, "entry 46 should remain")
          assert(e47.isDefined, "entry 47 should remain")

          // Entries after snapshot index should be deleted
          assert(e48.isEmpty, "entry 48 should be deleted")
          assert(e49.isEmpty, "entry 49 should be deleted")
          assert(e50.isEmpty, "entry 50 should be deleted")
        }
      }
    }
  }

  test("snapshot with different cluster configurations should work") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])

      config1 = ClusterConfiguration(leader, List(addr1, addr2))
      config2 = ClusterConfiguration(follower1, List(addr1, addr2, addr3))

      snapshot1 = Snapshot(10L, "config1-state", config1)
      snapshot2 = Snapshot(20L, "config2-state", config2)

      _          <- storage.persistSnapshot(snapshot1)
      retrieved1 <- storage.retrieveSnapshot

      _          <- storage.persistSnapshot(snapshot2)
      retrieved2 <- storage.retrieveSnapshot
    } yield {
      assertEquals(retrieved1.get.config.members.size, 2)
      assertEquals(retrieved1.get.config.currentNode, leader)

      assertEquals(retrieved2.get.config.members.size, 3)
      assertEquals(retrieved2.get.config.currentNode, follower1)
    }
  }

  test("getSnapshotMetadata should return correct metadata after snapshot creation") {
    for {
      log    <- IO(new InMemoryLog[IO, String])
      config <- log.membershipManager.getClusterConfiguration
      snapshot = Snapshot(75L, "metadata-test", config)

      _        <- log.snapshotStorage.persistSnapshot(snapshot)
      metadata <- log.getSnapshotMetadata()
    } yield {
      assert(metadata.isDefined)
      assertEquals(metadata.get._1, 75L)    // lastIndex
      assertEquals(metadata.get._2, config) // configuration
    }
  }

  test("getSnapshotMetadata should return None when no snapshot exists") {
    for {
      log      <- IO(new InMemoryLog[IO, String])
      metadata <- log.getSnapshotMetadata()
    } yield {
      assert(metadata.isEmpty)
    }
  }

  test("concurrent snapshot operations should be safe") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])

      // Create multiple snapshots concurrently
      snapshots = (1 to 10).map(i => Snapshot(i.toLong, s"state-$i", testConfig))

      // Persist all snapshots concurrently
      _ <- snapshots.map(storage.persistSnapshot).toList.parSequence

      // Should contain the last persisted snapshot (non-deterministic which one)
      finalSnapshot <- storage.retrieveSnapshot
    } yield {
      assert(finalSnapshot.isDefined)
      assert(snapshots.map(_.data).contains(finalSnapshot.get.data))
    }
  }

  test("snapshot restore should handle state machine errors gracefully") {
    withLogErrorHandling {
      for {
        log    <- IO(new InMemoryLog[IO, String])
        config <- log.membershipManager.getClusterConfiguration

        // This should work fine for String state machine
        validSnapshot = Snapshot(30L, "valid-state", config)
        result1 <- log.restoreSnapshot(validSnapshot).attempt
      } yield {
        assert(result1.isRight, "Valid snapshot should restore successfully")
      }
    }
  }

  test("empty snapshot data should be handled correctly") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      snapshot = Snapshot(5L, "", testConfig) // Empty string data

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.retrieveSnapshot
    } yield {
      assert(retrieved.isDefined)
      assertEquals(retrieved.get.data, "")
      assertEquals(retrieved.get.lastIndex, 5L)
    }
  }

  test("large snapshot data should be handled correctly") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, String])
      largeData = "x" * 10000 // 10K character string
      snapshot  = Snapshot(1000L, largeData, testConfig)

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.retrieveSnapshot
    } yield {
      assert(retrieved.isDefined)
      assertEquals(retrieved.get.data.length, 10000)
      assertEquals(retrieved.get.data, largeData)
    }
  }

  test("KV state machine snapshot store and restore") {
    withStateMachineErrorHandling {
      for {
        kvStorage      <- IO(new InMemorySnapshotStorage[IO, Map[Array[Byte], Array[Byte]]])
        kvStateMachine <- IO(new InMemoryKVStateMachine[IO])

        // Setup initial KV data
        key1   = "key1".getBytes
        value1 = "value1".getBytes
        key2   = "key2".getBytes
        value2 = "value2".getBytes

        _ <- kvStateMachine.put(key1, value1)
        _ <- kvStateMachine.put(key2, value2)

        // Get current state and create snapshot
        currentState <- kvStateMachine.getCurrentState
        snapshot = Snapshot(100L, currentState, testConfig)

        _ <- kvStorage.persistSnapshot(snapshot)

        // Clear the state machine and restore from snapshot
        _ <- kvStateMachine.delete(key1)
        _ <- kvStateMachine.delete(key2)

        // Verify data is cleared
        cleared1 <- kvStateMachine.get(key1)
        cleared2 <- kvStateMachine.get(key2)

        // Restore from snapshot
        retrieved <- kvStorage.retrieveSnapshot
        _         <- kvStateMachine.restoreSnapshot(retrieved.get.lastIndex, retrieved.get.data)

        // Verify data is restored
        restored1    <- kvStateMachine.get(key1)
        restored2    <- kvStateMachine.get(key2)
        appliedIndex <- kvStateMachine.appliedIndex
      } yield {
        // Verify data was cleared
        assert(cleared1.isEmpty)
        assert(cleared2.isEmpty)

        // Verify data was restored correctly
        assert(restored1.isDefined)
        assert(restored2.isDefined)
        assert(restored1.get.sameElements(value1))
        assert(restored2.get.sameElements(value2))
        assertEquals(appliedIndex, 100L)
      }
    }
  }

  test("snapshot storage with null data should be handled") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, Option[String]])
      snapshot: Snapshot[Option[String]] = Snapshot(1L, None, testConfig)

      _         <- storage.persistSnapshot(snapshot)
      retrieved <- storage.retrieveSnapshot
    } yield {
      assert(retrieved.isDefined)
      assertEquals(retrieved.get.data, None)
      assertEquals(retrieved.get.lastIndex, 1L)
    }
  }

  test("sequential snapshot operations should maintain consistency") {
    for {
      storage <- IO(new InMemorySnapshotStorage[IO, Int])

      // Create a sequence of snapshots
      snapshots = (1 to 5).map(i => Snapshot(i * 10L, i * 100, testConfig))

      // Persist them sequentially
      _ <- snapshots.foldLeft(IO.unit) { (acc, snapshot) =>
        acc.flatMap(_ => storage.persistSnapshot(snapshot))
      }

      // Should have the last snapshot
      finalSnapshot <- storage.retrieveSnapshot
    } yield {
      assert(finalSnapshot.isDefined)
      assertEquals(finalSnapshot.get.lastIndex, 50L)
      assertEquals(finalSnapshot.get.data, 500)
    }
  }
}
