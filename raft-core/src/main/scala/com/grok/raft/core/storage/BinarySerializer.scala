package com.grok.raft.core.storage

import java.nio.ByteBuffer
import com.grok.raft.core.protocol.{
  Command,
  ReadCommand,
  WriteCommand,
  WriteOp,
  ReadOp,
  Create,
  Update,
  Delete,
  Upsert,
  Get,
  Scan,
  Range,
  Keys
}
import com.grok.raft.core.internal.LogEntry

object BinarySerializer {

  def serializeLogEntry(entry: LogEntry): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    buffer.putLong(entry.term)
    buffer.putLong(entry.index)

    entry.command match {
      case writeOp: WriteOp[_, _] =>
        buffer.put(1.toByte) // WriteOp marker
        serializeWriteOp(writeOp, buffer)
      case readOp: ReadOp[_, _] =>
        buffer.put(2.toByte) // ReadOp marker
        serializeReadOp(readOp, buffer)
      case other =>
        buffer.put(0.toByte) // Other command marker
        val serialized = other.toString.getBytes("UTF-8")
        buffer.putInt(serialized.length)
        buffer.put(serialized)
    }

    val result = new Array[Byte](buffer.position())
    buffer.flip()
    buffer.get(result)
    result
  }

  def deserializeLogEntry(bytes: Array[Byte]): LogEntry = {
    val buffer      = ByteBuffer.wrap(bytes)
    val term        = buffer.getLong()
    val index       = buffer.getLong()
    val commandType = buffer.get()

    val command: Command[?] = commandType match {
      case 1 => deserializeWriteOp(buffer)
      case 2 => deserializeReadOp(buffer)
      case _ =>
        val length       = buffer.getInt()
        val commandBytes = new Array[Byte](length)
        buffer.get(commandBytes)
        // For unknown commands, return a simple Get operation as fallback
        Get(new String(commandBytes, "UTF-8"))
    }

    LogEntry(term, index, command)
  }

  private def serializeWriteOp[K, V](writeOp: WriteOp[K, V], buffer: ByteBuffer): Unit = {
    writeOp match {
      case Create(key, value) =>
        buffer.put(1.toByte)
        serializeKeyValue(key, value, buffer)
      case Update(key, value) =>
        buffer.put(2.toByte)
        serializeKeyValue(key, value, buffer)
      case Delete(key) =>
        buffer.put(3.toByte)
        serializeKey(key, buffer)
      case Upsert(key, value) =>
        buffer.put(4.toByte)
        serializeKeyValue(key, value, buffer)
    }
  }

  private def deserializeWriteOp(buffer: ByteBuffer): WriteOp[String, String] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val (key, value) = deserializeKeyValue(buffer)
        Create(key, value)
      case 2 =>
        val (key, value) = deserializeKeyValue(buffer)
        Update(key, value)
      case 3 =>
        val key = deserializeKey(buffer)
        Delete(key)
      case 4 =>
        val (key, value) = deserializeKeyValue(buffer)
        Upsert(key, value)
    }
  }

  private def serializeReadOp[K, V](readOp: ReadOp[K, V], buffer: ByteBuffer): Unit = {
    readOp match {
      case Get(key) =>
        buffer.put(1.toByte)
        serializeKey(key, buffer)
      case Scan(startKey, limit) =>
        buffer.put(2.toByte)
        serializeKey(startKey, buffer)
        buffer.putInt(limit): Unit
      case Range(startKey, endKey) =>
        buffer.put(3.toByte)
        serializeKey(startKey, buffer)
        serializeKey(endKey, buffer)
      case Keys(prefix) =>
        buffer.put(4.toByte)
        prefix match {
          case Some(p) =>
            buffer.put(1.toByte)
            serializeKey(p, buffer)
          case None =>
            buffer.put(0.toByte): Unit
        }
    }
  }

  private def deserializeReadOp(buffer: ByteBuffer): ReadOp[String, String] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val key = deserializeKey(buffer)
        Get(key)
      case 2 =>
        val startKey = deserializeKey(buffer)
        val limit    = buffer.getInt()
        Scan(startKey, limit)
      case 3 =>
        val startKey = deserializeKey(buffer)
        val endKey   = deserializeKey(buffer)
        Range(startKey, endKey)
      case 4 =>
        val hasPrefix = buffer.get()
        val prefix    = if (hasPrefix == 1) Some(deserializeKey(buffer)) else None
        Keys(prefix)
    }
  }

  private def serializeKey[K](key: K, buffer: ByteBuffer): Unit = {
    val keyBytes = key.toString.getBytes("UTF-8")
    buffer.putInt(keyBytes.length)
    buffer.put(keyBytes): Unit
  }

  private def serializeKeyValue[K, V](key: K, value: V, buffer: ByteBuffer): Unit = {
    serializeKey(key, buffer)
    val valueBytes = value.toString.getBytes("UTF-8")
    buffer.putInt(valueBytes.length)
    buffer.put(valueBytes): Unit
  }

  private def deserializeKey(buffer: ByteBuffer): String = {
    val keyLength = buffer.getInt()
    val keyBytes  = new Array[Byte](keyLength)
    buffer.get(keyBytes)
    new String(keyBytes, "UTF-8")
  }

  private def deserializeKeyValue(buffer: ByteBuffer): (String, String) = {
    val key         = deserializeKey(buffer)
    val valueLength = buffer.getInt()
    val valueBytes  = new Array[Byte](valueLength)
    buffer.get(valueBytes)
    val value = new String(valueBytes, "UTF-8")
    (key, value)
  }
}
