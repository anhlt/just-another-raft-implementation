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

  // String serializers for tests
  val keySerializer = TypedSerializer.forType(StringType)
  val valueSerializer = TypedSerializer.forType(StringType)

  test("KeyValueStateMachine should handle basic CRUD operations") {
    for {
      // Create in-memory storage for testing
      storage <- InMemoryKeyValueStorage[IO]()

      // Create applied index ref
      appliedIndexRef <- Ref[IO].of(0L)

      // Create the state machine  
      stateMachine = KeyValueStateMachine.apply[IO, String, String](storage, appliedIndexRef, keySerializer, valueSerializer)

      // Test Create operation
      createResultAny <- stateMachine.applyWrite((1L, Create(TypedValue("key1", StringType), TypedValue("value1", StringType))))
      createResult = createResultAny.asInstanceOf[Option[String]]
      _            = assertEquals(createResult, Some("value1"))

      // Test Read operation
      readResultAny <- stateMachine.applyRead(Get(TypedValue("key1", StringType)))
      readResult = readResultAny.asInstanceOf[Option[String]]
      _          = assertEquals(readResult, Some("value1"))

      // Test Update operation
      updateResultAny <- stateMachine.applyWrite((2L, Update(TypedValue("key1", StringType), TypedValue("updated-value", StringType))))
      updateResult = updateResultAny.asInstanceOf[Option[String]]
      _            = assertEquals(updateResult, Some("updated-value"))

      // Verify update worked
      readAfterUpdateAny <- stateMachine.applyRead(Get(TypedValue("key1", StringType)))
      readAfterUpdate = readAfterUpdateAny.asInstanceOf[Option[String]]
      _               = assertEquals(readAfterUpdate, Some("updated-value"))

      // Test key that doesn't exist
      nonExistentReadAny <- stateMachine.applyRead(Get(TypedValue("non-existent", StringType)))
      nonExistentRead = nonExistentReadAny.asInstanceOf[Option[String]]
      _               = assertEquals(nonExistentRead, None)

      // Test Delete operation
      deleteResultAny <- stateMachine.applyWrite((3L, Delete(TypedValue("key1", StringType))))
      deleteResult = deleteResultAny.asInstanceOf[Option[String]]
      _            = assertEquals(deleteResult, Some("updated-value")) // Returns old value

      // Verify deletion worked
      readAfterDeleteAny <- stateMachine.applyRead(Get(TypedValue("key1", StringType)))
      readAfterDelete = readAfterDeleteAny.asInstanceOf[Option[String]]
      _               = assertEquals(readAfterDelete, None)

      // Cleanup
      _ <- storage.close()
    } yield ()
  }

  test("KeyValueStateMachine should handle scan operations") {
    for {
      storage         <- InMemoryKeyValueStorage[IO]()
      appliedIndexRef <- Ref[IO].of(0L)
      stateMachine = KeyValueStateMachine.apply[IO, String, String](storage, appliedIndexRef, keySerializer, valueSerializer)

      // Insert test data
      _ <- stateMachine.applyWrite((1L, Create(TypedValue("apple", StringType), TypedValue("fruit", StringType))))
      _ <- stateMachine.applyWrite((2L, Create(TypedValue("banana", StringType), TypedValue("fruit", StringType))))
      _ <- stateMachine.applyWrite((3L, Create(TypedValue("carrot", StringType), TypedValue("vegetable", StringType))))
      _ <- stateMachine.applyWrite((4L, Create(TypedValue("date", StringType), TypedValue("fruit", StringType))))

      // Test range operation (scan between two keys)
      rangeResultAny <- stateMachine.applyRead(Range(TypedValue("apple", StringType), TypedValue("carrot", StringType)))
      rangeResult = rangeResultAny.asInstanceOf[List[String]]
      _           = assert(rangeResult.nonEmpty, "Range should return results")
      _ = assert(rangeResult.contains("fruit"), s"Should contain fruit, got: ${rangeResult}")
      _ = assert(rangeResult.size == 2, s"Should return 2 values, got: ${rangeResult.size}")
      
      // Verify it's the values from apple and banana keys (both are "fruit")
      _ = assert(rangeResult.forall(_ == "fruit"), s"All values should be 'fruit', got: ${rangeResult}")

      // Test scan operation with limit
      scanResultAny <- stateMachine.applyRead(Scan(TypedValue("apple", StringType), 2))
      scanResult = scanResultAny.asInstanceOf[List[String]].mkString(",")
      _          = assert(scanResult.nonEmpty, "Scan should return results")
      _          = assert(scanResult.contains("fruit"), s"Should contain fruit, got: ${scanResult}")

      // Test keys operation
      keysResultAny <- stateMachine.applyRead(Keys(Some(TypedValue("a", StringType)))) // Keys starting with "a"
      keysResult = keysResultAny.asInstanceOf[List[String]]
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
      stateMachine = KeyValueStateMachine.apply[IO, String, String](storage, appliedIndexRef, keySerializer, valueSerializer)

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
      _ = assert(scanResult.nonEmpty, "Scan should return results")
      _ = assert(scanResult.contains("Jane Doe"), s"Should contain Jane Doe, got: $scanResult")
      _ = assert(scanResult.contains("Alice Smith"), s"Should contain Alice Smith, got: $scanResult")

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
      stateMachine = KeyValueStateMachine.apply[IO, String, String](storage, appliedIndexRef, keySerializer, valueSerializer)

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
      stateMachine = KeyValueStateMachine.apply[IO, String, String](storage, appliedIndexRef, keySerializer, valueSerializer)

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
      _ = assertEquals(noResultsScan, List.empty[String])

      // Test keys with no results
      noResultsKeys <- kvRaft.keys(Some("zzz-nonexistent:prefix:"), bypassConsensus = true)
      _ = assertEquals(noResultsKeys, List.empty[String])

      // Test scan with limit 0 (should return no results)
      zeroLimitScan <- kvRaft.scan("", 0, bypassConsensus = true)
      _ = assertEquals(zeroLimitScan, List.empty[String])

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
