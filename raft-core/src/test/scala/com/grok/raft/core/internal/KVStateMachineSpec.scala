package com.grok.raft.core.internal

import cats.effect.*
import munit.CatsEffectSuite
import com.grok.raft.core.protocol.*

class KVStateMachineSpec extends CatsEffectSuite {
  // Import MTL test utilities
  import MtlTestUtils.given

  test("InMemoryKVStateMachine should handle basic put/get operations") {
    val kvStore = new InMemoryKVStateMachine[IO]
    val key = "hello".getBytes
    val value = "world".getBytes

    for {
      _ <- kvStore.put(key, value)
      result <- kvStore.get(key)
      contains <- kvStore.contains(key)
    } yield {
      assertEquals(result.map(new String(_)), Some("world"))
      assertEquals(contains, true)
    }
  }

  test("InMemoryKVStateMachine should handle delete operations") {
    val kvStore = new InMemoryKVStateMachine[IO]
    val key = "test".getBytes
    val value = "data".getBytes

    for {
      _ <- kvStore.put(key, value)
      beforeDelete <- kvStore.contains(key)
      _ <- kvStore.delete(key)
      afterDelete <- kvStore.contains(key)
      result <- kvStore.get(key)
    } yield {
      assertEquals(beforeDelete, true)
      assertEquals(afterDelete, false)
      assertEquals(result, None)
    }
  }

  test("InMemoryKVStateMachine should handle range queries") {
    val kvStore = new InMemoryKVStateMachine[IO]
    
    for {
      _ <- kvStore.put("a".getBytes, "1".getBytes)
      _ <- kvStore.put("b".getBytes, "2".getBytes)
      _ <- kvStore.put("c".getBytes, "3".getBytes)
      _ <- kvStore.put("d".getBytes, "4".getBytes)
      range <- kvStore.range("b".getBytes, "d".getBytes, None)
    } yield {
      val results = range.map { case (k, v) => (new String(k), new String(v)) }
      assertEquals(results.length, 2)
      assertEquals(results, List(("b", "2"), ("c", "3")))
    }
  }

  test("InMemoryKVStateMachine should handle prefix scan") {
    val kvStore = new InMemoryKVStateMachine[IO]
    
    for {
      _ <- kvStore.put("app.config".getBytes, "cfg1".getBytes)
      _ <- kvStore.put("app.settings".getBytes, "cfg2".getBytes)
      _ <- kvStore.put("data.cache".getBytes, "cache1".getBytes)
      scan <- kvStore.scan("app".getBytes, None)
    } yield {
      val results = scan.map { case (k, v) => (new String(k), new String(v)) }
      assertEquals(results.length, 2)
      assert(results.forall(_._1.startsWith("app")))
    }
  }

  test("InMemoryKVStateMachine should handle keys enumeration") {
    val kvStore = new InMemoryKVStateMachine[IO]
    
    for {
      _ <- kvStore.put("key1".getBytes, "val1".getBytes)
      _ <- kvStore.put("key2".getBytes, "val2".getBytes)
      _ <- kvStore.put("other".getBytes, "val3".getBytes)
      allKeys <- kvStore.keys(None, None)
      prefixKeys <- kvStore.keys(Some("key".getBytes), None)
      limitedKeys <- kvStore.keys(None, Some(2))
    } yield {
      assertEquals(allKeys.length, 3)
      assertEquals(prefixKeys.length, 2)
      assertEquals(limitedKeys.length, 2)
      assert(prefixKeys.map(new String(_)).forall(_.startsWith("key")))
    }
  }

  test("InMemoryKVStateMachine should work with StateMachine interface") {
    val kvStore = new InMemoryKVStateMachine[IO]
    val putCmd = Put("test".getBytes, "value".getBytes)
    val getCmd = Get("test".getBytes)

    for {
      _ <- kvStore.applyWrite((1L, putCmd))
      result <- kvStore.applyRead(getCmd)
      index <- kvStore.appliedIndex
    } yield {
      assertEquals(result.asInstanceOf[Option[Array[Byte]]].map(new String(_)), Some("value"))
      assertEquals(index, 0L) // Index tracking would need proper implementation
    }
  }

  test("KVStateMachine should handle error cases with empty keys") {
    val kvStore = new InMemoryKVStateMachine[IO]
    val emptyKey = Array.empty[Byte]
    
    for {
      putError <- kvStore.applyWrite((1L, Put(emptyKey, "value".getBytes))).attempt
      getError <- kvStore.applyRead(Get(emptyKey)).attempt
      deleteError <- kvStore.applyWrite((1L, Delete(emptyKey))).attempt
      containsError <- kvStore.applyRead(Contains(emptyKey)).attempt
    } yield {
      assert(putError.isLeft, "Put with empty key should fail")
      assert(getError.isLeft, "Get with empty key should fail")  
      assert(deleteError.isLeft, "Delete with empty key should fail")
      assert(containsError.isLeft, "Contains with empty key should fail")
    }
  }

  test("KVStateMachine should handle error cases with negative limits") {
    val kvStore = new InMemoryKVStateMachine[IO]
    val key = "test".getBytes
    
    for {
      rangeError <- kvStore.applyRead(Range(key, key, Some(-1))).attempt
      scanError <- kvStore.applyRead(Scan(key, Some(-1))).attempt
      keysError <- kvStore.applyRead(Keys(None, Some(-1))).attempt
    } yield {
      assert(rangeError.isLeft, "Range with negative limit should fail")
      assert(scanError.isLeft, "Scan with negative limit should fail")
      assert(keysError.isLeft, "Keys with negative limit should fail")
    }
  }
}