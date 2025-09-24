package com.grok.raft.core

import cats.effect.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.*
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class KeyValueRaftSpec extends CatsEffectSuite {

  // Create a logger for the tests
  given Logger[IO] = Slf4jLogger.getLogger[IO]

  test("KeyValueStateMachine should handle basic CRUD operations") {
    for {
      // Create in-memory storage for testing
      storage <- InMemoryKeyValueStorage[IO]()

      // Create applied index ref
      appliedIndexRef <- Ref[IO].of(0L)

      // Create the state machine
      stateMachine = KeyValueStateMachine.apply[IO](storage, appliedIndexRef)

      // Test Create operation
      createResultAny <- stateMachine.applyWrite((1L, Create("key1".getBytes("UTF-8"), "value1".getBytes("UTF-8"))))
      createResult = createResultAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _            = assertEquals(createResult, Some("value1"))

      // Test Read operation
      readResultAny <- stateMachine.applyRead(Get("key1".getBytes("UTF-8")))
      readResult = readResultAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _          = assertEquals(readResult, Some("value1"))

      // Test Update operation
      updateResultAny <- stateMachine.applyWrite((2L, Update("key1".getBytes("UTF-8"), "updated-value".getBytes("UTF-8"))))
      updateResult = updateResultAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _            = assertEquals(updateResult, Some("updated-value"))

      // Verify update worked
      readAfterUpdateAny <- stateMachine.applyRead(Get("key1".getBytes("UTF-8")))
      readAfterUpdate = readAfterUpdateAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _               = assertEquals(readAfterUpdate, Some("updated-value"))

      // Test key that doesn't exist
      nonExistentReadAny <- stateMachine.applyRead(Get("non-existent".getBytes("UTF-8")))
      nonExistentRead = nonExistentReadAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _               = assertEquals(nonExistentRead, None)

      // Test Delete operation
      deleteResultAny <- stateMachine.applyWrite((3L, Delete("key1".getBytes("UTF-8"))))
      deleteResult = deleteResultAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _            = assertEquals(deleteResult, Some("updated-value")) // Returns old value

      // Verify deletion worked
      readAfterDeleteAny <- stateMachine.applyRead(Get("key1".getBytes("UTF-8")))
      readAfterDelete = readAfterDeleteAny.asInstanceOf[Option[Array[Byte]]].map(new String(_, "UTF-8"))
      _               = assertEquals(readAfterDelete, None)

      // Cleanup
      _ <- storage.close()
    } yield ()
  }

  test("KeyValueStateMachine should handle scan operations") {
    for {
      storage         <- InMemoryKeyValueStorage[IO]()
      appliedIndexRef <- Ref[IO].of(0L)
      stateMachine = KeyValueStateMachine.apply[IO](storage, appliedIndexRef)

      // Insert test data
      _ <- stateMachine.applyWrite((1L, Create("apple".getBytes("UTF-8"), "fruit".getBytes("UTF-8"))))
      _ <- stateMachine.applyWrite((2L, Create("banana".getBytes("UTF-8"), "fruit".getBytes("UTF-8"))))
      _ <- stateMachine.applyWrite((3L, Create("carrot".getBytes("UTF-8"), "vegetable".getBytes("UTF-8"))))
      _ <- stateMachine.applyWrite((4L, Create("date".getBytes("UTF-8"), "fruit".getBytes("UTF-8"))))

      // Test range operation (scan between two keys)
      rangeResultAny <- stateMachine.applyRead(Range("apple".getBytes("UTF-8"), "carrot".getBytes("UTF-8")))
      rangeResult = rangeResultAny.asInstanceOf[List[Array[Byte]]].map(new String(_, "UTF-8")).mkString(",")
      _           = assert(rangeResult.nonEmpty, "Range should return results")
      _ = assert(rangeResult.contains("apple"), s"Should contain apple, got: ${rangeResult}")
      _ = assert(rangeResult.contains("banana"), s"Should contain banana, got: ${rangeResult}")

      // Test scan operation with limit
      scanResultAny <- stateMachine.applyRead(Scan("apple".getBytes("UTF-8"), 2))
      scanResult = scanResultAny.asInstanceOf[List[Array[Byte]]].map(new String(_, "UTF-8")).mkString(",")
      _          = assert(scanResult.nonEmpty, "Scan should return results")
      _          = assert(scanResult.contains("apple"), s"Should contain apple, got: ${scanResult}")

      // Test keys operation
      keysResultAny <- stateMachine.applyRead(Keys(Some("a".getBytes("UTF-8")))) // Keys starting with "a"
      keysResult = keysResultAny.asInstanceOf[List[Array[Byte]]].map(new String(_, "UTF-8"))
      _          = assert(keysResult.nonEmpty, "Keys should return results")
      _          = assert(keysResult.contains("apple"), s"Should contain apple key, got: ${keysResult}")

      _ <- storage.close()
    } yield ()
  }

  test("KeyValueRaft methods should work with direct instantiation") {
    for {
      // Create components directly instead of using the complex factory
      storage         <- InMemoryKeyValueStorage[IO]()
      appliedIndexRef <- Ref[IO].of(0L)
      stateMachine = KeyValueStateMachine.apply[IO](storage, appliedIndexRef)

      // Create mock configuration (minimal setup for testing)
      mockNodeAddress = NodeAddress("127.0.0.1", 8080)
      mockNode        = Follower(mockNodeAddress, 1L)
      mockConfig      = ClusterConfiguration(mockNode, List(mockNodeAddress))

      // Create a KeyValueRaft instance directly (bypassing complex dependencies)
      kvRaft = new KeyValueRaft[IO](stateMachine, storage)

      // Test put operation (upsert functionality)
      putResult1 <- kvRaft.put("user:123", "John Doe")
      _ = assertEquals(putResult1, Some("John Doe"))

      // Test get operation with bypass consensus (direct read)
      getResult1 <- kvRaft.get("user:123", bypassConsensus = true)
      _ = assertEquals(getResult1, Some("John Doe"))

      // Test get non-existent key
      nonExistentResult <- kvRaft.get("user:999", bypassConsensus = true)
      _ = assertEquals(nonExistentResult, None)

      // Test update via put (should replace existing value)
      putResult2 <- kvRaft.put("user:123", "Jane Doe")
      _ = assertEquals(putResult2, Some("Jane Doe"))

      // Verify update worked with bypass read
      updatedGetResult <- kvRaft.get("user:123", bypassConsensus = true)
      _ = assertEquals(updatedGetResult, Some("Jane Doe"))

      // Add more test data for scanning
      _ <- kvRaft.put("user:124", "Alice Smith")
      _ <- kvRaft.put("user:125", "Bob Wilson")
      _ <- kvRaft.put("product:1", "Laptop")
      _ <- kvRaft.put("product:2", "Mouse")

      // Test scan operation with bypass consensus
      scanResult <- kvRaft.scan("user:", 3, bypassConsensus = true)
      _ = assert(scanResult.isDefined, "Scan should return results")
      _ = assert(scanResult.get.contains("user:123"), s"Should contain user:123, got: ${scanResult.get}")
      _ = assert(scanResult.get.contains("user:124"), s"Should contain user:124, got: ${scanResult.get}")

      // Test keys operation with prefix
      keysResult <- kvRaft.keys(Some("user:"), bypassConsensus = true)
      _ = assert(keysResult.nonEmpty, "Keys should return results")
      _ = assert(keysResult.contains("user:123"), s"Should contain user:123 key, got: ${keysResult}")
      _ = assert(keysResult.contains("user:124"), s"Should contain user:124 key, got: ${keysResult}")

      // Test delete operation
      deleteResult <- kvRaft.delete("user:125")
      _ = assertEquals(deleteResult, Some("Bob Wilson")) // Should return old value

      // Verify deletion with bypass read
      deletedGetResult <- kvRaft.get("user:125", bypassConsensus = true)
      _ = assertEquals(deletedGetResult, None)

      // Test delete non-existent key
      nonExistentDeleteResult <- kvRaft.delete("user:999")
      _ = assertEquals(nonExistentDeleteResult, None)

      // Cleanup
      _ <- kvRaft.close()
    } yield ()
  }

  test("Bypass consensus reads should work correctly and consistently") {
    for {
      storage         <- InMemoryKeyValueStorage[IO]()
      appliedIndexRef <- Ref[IO].of(0L)
      stateMachine = KeyValueStateMachine.apply[IO](storage, appliedIndexRef)

      mockNodeAddress = NodeAddress("127.0.0.1", 8080)
      mockNode        = Follower(mockNodeAddress, 1L)
      mockConfig      = ClusterConfiguration(mockNode, List(mockNodeAddress))
      kvRaft          = new KeyValueRaft[IO](stateMachine, storage)

      // Insert some test data
      _ <- kvRaft.put("test:1", "value1")
      _ <- kvRaft.put("test:2", "value2")
      _ <- kvRaft.put("test:3", "value3")

      // Test consistency between consensus and bypass reads
      consensusRead1 <- kvRaft.get("test:1", bypassConsensus = false)
      bypassRead1    <- kvRaft.get("test:1", bypassConsensus = true)
      _ = assertEquals(consensusRead1, bypassRead1, "Consensus and bypass reads should be consistent")

      consensusRead2 <- kvRaft.get("test:2", bypassConsensus = false)
      bypassRead2    <- kvRaft.get("test:2", bypassConsensus = true)
      _ = assertEquals(consensusRead2, bypassRead2, "Consensus and bypass reads should be consistent")

      // Test non-existent key consistency
      consensusReadMissing <- kvRaft.get("missing:key", bypassConsensus = false)
      bypassReadMissing    <- kvRaft.get("missing:key", bypassConsensus = true)
      _ = assertEquals(consensusReadMissing, bypassReadMissing, "Both reads should return None for missing keys")

      // Test bypass scan vs consensus scan consistency
      consensusScan <- kvRaft.scan("test:", 10, bypassConsensus = false)
      bypassScan    <- kvRaft.scan("test:", 10, bypassConsensus = true)
      _ = assertEquals(consensusScan, bypassScan, "Consensus and bypass scans should be consistent")

      // Test bypass keys vs consensus keys consistency
      consensusKeys <- kvRaft.keys(Some("test:"), bypassConsensus = false)
      bypassKeys    <- kvRaft.keys(Some("test:"), bypassConsensus = true)
      _ = assertEquals(consensusKeys, bypassKeys, "Consensus and bypass keys operations should be consistent")

      _ <- kvRaft.close()
    } yield ()
  }

  test("KeyValueRaft should handle edge cases and error scenarios") {
    for {
      storage         <- InMemoryKeyValueStorage[IO]()
      appliedIndexRef <- Ref[IO].of(0L)
      stateMachine = KeyValueStateMachine.apply[IO](storage, appliedIndexRef)

      mockNodeAddress = NodeAddress("127.0.0.1", 8080)
      mockNode        = Follower(mockNodeAddress, 1L)
      mockConfig      = ClusterConfiguration(mockNode, List(mockNodeAddress))
      kvRaft          = new KeyValueRaft[IO](stateMachine, storage)

      // Test empty key and value handling
      emptyKeyResult <- kvRaft.put("", "empty-key-value")
      _ = assertEquals(emptyKeyResult, Some("empty-key-value"))

      emptyValueResult <- kvRaft.put("empty-value-key", "")
      _ = assertEquals(emptyValueResult, Some(""))

      // Verify empty key/value reads work
      emptyKeyRead <- kvRaft.get("", bypassConsensus = true)
      _ = assertEquals(emptyKeyRead, Some("empty-key-value"))

      emptyValueRead <- kvRaft.get("empty-value-key", bypassConsensus = true)
      _ = assertEquals(emptyValueRead, Some(""))

      // Test special characters in keys and values
      specialKeyResult <- kvRaft.put("key:with:colons/and/slashes", "special characters: @#$%^&*()")
      _ = assertEquals(specialKeyResult, Some("special characters: @#$%^&*()"))

      specialKeyRead <- kvRaft.get("key:with:colons/and/slashes", bypassConsensus = true)
      _ = assertEquals(specialKeyRead, Some("special characters: @#$%^&*()"))

      // Test Unicode support
      unicodeResult <- kvRaft.put("unicode-key-🔑", "Unicode value: 你好世界 🌍")
      _ = assertEquals(unicodeResult, Some("Unicode value: 你好世界 🌍"))

      unicodeRead <- kvRaft.get("unicode-key-🔑", bypassConsensus = true)
      _ = assertEquals(unicodeRead, Some("Unicode value: 你好世界 🌍"))

      // Test scan with no results (prefix that doesn't exist and is lexicographically after all keys)
      noResultsScan <- kvRaft.scan("zzz-nonexistent:prefix:", 10, bypassConsensus = true)
      _ = assertEquals(noResultsScan, None)

      // Test keys with no results
      noResultsKeys <- kvRaft.keys(Some("zzz-nonexistent:prefix:"), bypassConsensus = true)
      _ = assertEquals(noResultsKeys, List.empty[String])

      // Test scan with limit 0 (should return no results)
      zeroLimitScan <- kvRaft.scan("", 0, bypassConsensus = true)
      _ = assertEquals(zeroLimitScan, None)

      // Test very long key and value
      longKey   = "very-long-key-" + "x" * 1000
      longValue = "very-long-value-" + "y" * 5000
      longResult <- kvRaft.put(longKey, longValue)
      _ = assertEquals(longResult, Some(longValue))

      longRead <- kvRaft.get(longKey, bypassConsensus = true)
      _ = assertEquals(longRead, Some(longValue))

      _ <- kvRaft.close()
    } yield ()
  }
}
