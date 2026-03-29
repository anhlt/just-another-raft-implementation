package com.grok.raft.effects.storage

import munit.CatsEffectSuite
import cats.effect.*
import cats.mtl.Raise
import com.grok.raft.core.error.StateMachineError
import com.grok.raft.core.protocol.Put
import com.grok.raft.core.internal.LogEntry
import com.grok.raft.core.internal.{NodeAddress, Follower}
import com.grok.raft.core.storage.PersistedState
import com.grok.raft.core.ClusterConfiguration
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.nio.file.Files

class RocksDBStorageSpec extends CatsEffectSuite:

  given logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]
  
  // Helper to compare LogEntries with byte array content comparison
  def assertLogEntryEquals(actual: Option[LogEntry], expected: LogEntry): Unit = 
    actual match
      case Some(actualEntry) =>
        assertEquals(actualEntry.index, expected.index)
        assertEquals(actualEntry.term, expected.term)
        (actualEntry.command, expected.command) match
          case (Put(k1, v1), Put(k2, v2)) =>
            assert(java.util.Arrays.equals(k1, k2), s"Keys don't match: ${new String(k1)} vs ${new String(k2)}")
            assert(java.util.Arrays.equals(v1, v2), s"Values don't match: ${new String(v1)} vs ${new String(v2)}")
          case _ => assertEquals(actualEntry.command, expected.command)
      case None => fail(s"Expected Some($expected) but got None")
  
  // Simple Raise instance for StateMachineError
  given raiseStateMachineError[F[_]: Sync]: Raise[F, StateMachineError] = new Raise[F, StateMachineError] {
    def functor: cats.Functor[F] = summon[Sync[F]]
    def raise[E2 <: StateMachineError, A](error: E2): F[A] = 
      Sync[F].raiseError(new RuntimeException(s"StateMachineError: $error"))
  }

  val tempDir = ResourceFunFixture { 
    Resource.make(IO(Files.createTempDirectory("rocksdb-test"))) { dir =>
      IO(deleteRecursively(dir.toFile))
    }
  }

  tempDir.test("RocksDBLogStorage should store and retrieve log entries") { dir =>
    val config = RocksDBConfig.default(dir)
    
    RocksDBLogStorage.create[IO](config).use { logStorage =>
      for
        // Test put and get
        entry1 <- IO.pure(LogEntry(1L, 1L, Put("key1".getBytes, "value1".getBytes)))
        _ <- logStorage.put(1L, entry1)
        
        retrieved <- logStorage.get(1L)
        _ = assertLogEntryEquals(retrieved, entry1)
        
        // Test last index
        lastIndex <- logStorage.lastIndex
        _ = assertEquals(lastIndex, 1L)
        
        // Test non-existent entry
        nonExistent <- logStorage.get(999L)
        _ = assertEquals(nonExistent, None)
        
        // Test multiple entries
        entry2 <- IO.pure(LogEntry(1L, 2L, Put("key2".getBytes, "value2".getBytes)))
        _ <- logStorage.put(2L, entry2)
        
        lastIndex2 <- logStorage.lastIndex
        _ = assertEquals(lastIndex2, 2L)
        
        // Test deleteAfter
        _ <- logStorage.deleteAfter(1L)
        deleted <- logStorage.get(2L)
        _ = assertEquals(deleted, None)
        
        stillExists <- logStorage.get(1L)
        _ = assertLogEntryEquals(stillExists, entry1)
        
      yield ()
    }
  }

  tempDir.test("RocksDBStateStorage should persist and retrieve state") { dir =>
    val config = RocksDBConfig.default(dir)
    
    RocksDBStateStorage.create[IO](config).use { stateStorage =>
      for
        // Test initial empty state
        initialState <- stateStorage.retrieveState
        _ = assertEquals(initialState, None)
        
        // Test persist and retrieve
        nodeAddr = NodeAddress("node1", 8080)
        persistedState = PersistedState(5L, Some(nodeAddr), 10L)
        _ <- stateStorage.persistState(persistedState)
        
        retrieved <- stateStorage.retrieveState
        _ = assertEquals(retrieved, Some(persistedState))
        
        // Test update state
        updatedState = persistedState.copy(term = 6L, appliedIndex = 11L)
        _ <- stateStorage.persistState(updatedState)
        
        retrievedUpdated <- stateStorage.retrieveState
        _ = assertEquals(retrievedUpdated, Some(updatedState))
        
      yield ()
    }
  }

  tempDir.test("RocksDBKVStateMachine should support basic KV operations") { dir =>
    val config = RocksDBConfig.default(dir)
    
    RocksDBKVStateMachine.create[IO](config).use { kvStore =>
      for
        // Test put and get
        _ <- kvStore.put("key1".getBytes, "value1".getBytes)
        value1 <- kvStore.get("key1".getBytes)
        _ = assertEquals(value1.map(new String(_)), Some("value1"))
        
        // Test contains
        exists <- kvStore.contains("key1".getBytes)
        _ = assertEquals(exists, true)
        
        notExists <- kvStore.contains("nonexistent".getBytes)
        _ = assertEquals(notExists, false)
        
        // Test delete
        _ <- kvStore.delete("key1".getBytes)
        deleted <- kvStore.get("key1".getBytes)
        _ = assertEquals(deleted, None)
        
        // Test range operations
        _ <- kvStore.put("a".getBytes, "1".getBytes)
        _ <- kvStore.put("b".getBytes, "2".getBytes)
        _ <- kvStore.put("c".getBytes, "3".getBytes)
        
        range <- kvStore.range("a".getBytes, "c".getBytes, Some(2))
        _ = assertEquals(range.length, 2)
        _ = assertEquals(new String(range(0)._1), "a")
        _ = assertEquals(new String(range(0)._2), "1")
        
        // Test scan with prefix
        _ <- kvStore.put("test1".getBytes, "val1".getBytes)
        _ <- kvStore.put("test2".getBytes, "val2".getBytes)
        _ <- kvStore.put("other".getBytes, "val3".getBytes)
        
        scanResults <- kvStore.scan("test".getBytes, None)
        _ = assertEquals(scanResults.length, 2)
        
      yield ()
    }
  }

  tempDir.test("RocksDBSnapshotStorage should create and compress snapshots") { dir =>
    val config = RocksDBConfig.default(dir)
    
    RocksDBKVStateMachine.create[IO](config).use { kvStore =>
      for
        // Add some test data
        _ <- kvStore.put("snap1".getBytes, "value1".getBytes)
        _ <- kvStore.put("snap2".getBytes, "value2".getBytes)
        
        snapshotStorage <- RocksDBSnapshotStorage.create[IO](
          kvStore.db,
          kvStore.kvDataCF,
          config
        )
        
        clusterConfig = ClusterConfiguration(
          Follower(NodeAddress("test", 8080), 1L),
          List(NodeAddress("test", 8080))
        )
        
        // Test snapshot creation
        rocksDbSnapshot <- snapshotStorage.createRocksDBSnapshot(100L, clusterConfig)
        _ = assertEquals(rocksDbSnapshot.lastIndex, 100L)
        _ = assert(Files.exists(rocksDbSnapshot.checkpointPath))
        
        // Test compression
        compressedSnapshot <- snapshotStorage.compressSnapshot(rocksDbSnapshot)
        _ = assertEquals(compressedSnapshot.lastIndex, 100L)
        _ = assert(compressedSnapshot.compressedData.nonEmpty)
        
        // Verify checkpoint directory was cleaned up
        _ = assert(!Files.exists(rocksDbSnapshot.checkpointPath))
        
      yield ()
    }
  }

  tempDir.test("RocksDBStorageManager should coordinate all storage components") { dir =>
    val config = RocksDBConfig.default(dir)
    
    RocksDBStorageManager.create[IO](config).use { storageManager =>
      for
        // Test log storage
        entry <- IO.pure(LogEntry(1L, 1L, Put("key1".getBytes, "value1".getBytes)))
        _ <- storageManager.logStorage.put(1L, entry)
        retrieved <- storageManager.logStorage.get(1L)
        _ = assertLogEntryEquals(retrieved, entry)
        
        // Test state storage
        nodeAddr = NodeAddress("node1", 8080)
        state = PersistedState(1L, Some(nodeAddr), 0L)
        _ <- storageManager.stateStorage.persistState(state)
        retrievedState <- storageManager.stateStorage.retrieveState
        _ = assertEquals(retrievedState, Some(state))
        
        // Test KV state machine
        _ <- storageManager.kvStateMachine.put("test".getBytes, "value".getBytes)
        kvValue <- storageManager.kvStateMachine.get("test".getBytes)
        _ = assertEquals(kvValue.map(new String(_)), Some("value"))
        
        // Test snapshot creation
        followerNode = Follower(nodeAddr, 1L)
        clusterConfig = ClusterConfiguration(followerNode, List(nodeAddr))
        compressedSnapshot <- storageManager.createSnapshot(1L, clusterConfig)
        _ = assertEquals(compressedSnapshot.lastIndex, 1L)
        _ = assert(compressedSnapshot.compressedData.nonEmpty)
        
      yield ()
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if file.isDirectory then
      file.listFiles().foreach(deleteRecursively)
    file.delete(): Unit
  }