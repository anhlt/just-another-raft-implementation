package com.grok.raft.core.storage

import com.grok.raft.core.protocol.{
  Command,
  WriteCommand,
  ReadCommand,
  Create,
  Update,
  Delete,
  Upsert,
  Get,
  Scan,
  Range,
  Keys,
  TypedValue,
  StringType
}
import com.grok.raft.core.internal.LogEntry

trait BinarySerializable {
  def serializeLogEntry(entry: LogEntry): Array[Byte]
  def deserializeLogEntry(bytes: Array[Byte]): LogEntry
}

object DefaultBinarySerializer extends BinarySerializable {
  import java.nio.ByteBuffer

  def serializeLogEntry(entry: LogEntry): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    buffer.putLong(entry.term)
    buffer.putLong(entry.index)

    entry.command match {
      case writeOp: WriteCommand[String, String, ?] =>
        buffer.put(1.toByte)
        serializeWriteCommand(writeOp, buffer)
      case readOp: ReadCommand[String, String, ?] =>
        buffer.put(2.toByte)
        serializeReadCommand(readOp, buffer)
      case other =>
        buffer.put(0.toByte)
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

    val command: Command[String, String, ?] = commandType match {
      case 1 => deserializeWriteCommand(buffer)
      case 2 => deserializeReadCommand(buffer)
      case _ =>
        val length       = buffer.getInt()
        val commandBytes = new Array[Byte](length)
        buffer.get(commandBytes)
        Get(TypedValue(new String(commandBytes, "UTF-8"), StringType))
    }

    LogEntry(term, index, command)
  }

  private def serializeWriteCommand(writeCmd: WriteCommand[String, String, ?], buffer: ByteBuffer): Unit = {
    writeCmd match {
      case Create(key, value) =>
        buffer.put(1.toByte)
        serializeByteArray(key.value.getBytes("UTF-8"), buffer)
        serializeByteArray(value.value.getBytes("UTF-8"), buffer)
      case Update(key, value) =>
        buffer.put(2.toByte)
        serializeByteArray(key.value.getBytes("UTF-8"), buffer)
        serializeByteArray(value.value.getBytes("UTF-8"), buffer)
      case Delete(key) =>
        buffer.put(3.toByte)
        serializeByteArray(key.value.getBytes("UTF-8"), buffer)
      case Upsert(key, value) =>
        buffer.put(4.toByte)
        serializeByteArray(key.value.getBytes("UTF-8"), buffer)
        serializeByteArray(value.value.getBytes("UTF-8"), buffer)
    }
  }

  private def deserializeWriteCommand(buffer: ByteBuffer): WriteCommand[String, String, ?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val key   = new String(deserializeByteArray(buffer), "UTF-8")
        val value = new String(deserializeByteArray(buffer), "UTF-8")
        Create(TypedValue(key, StringType), TypedValue(value, StringType))
      case 2 =>
        val key   = new String(deserializeByteArray(buffer), "UTF-8")
        val value = new String(deserializeByteArray(buffer), "UTF-8")
        Update(TypedValue(key, StringType), TypedValue(value, StringType))
      case 3 =>
        val key = new String(deserializeByteArray(buffer), "UTF-8")
        Delete(TypedValue(key, StringType))
      case 4 =>
        val key   = new String(deserializeByteArray(buffer), "UTF-8")
        val value = new String(deserializeByteArray(buffer), "UTF-8")
        Upsert(TypedValue(key, StringType), TypedValue(value, StringType))
    }
  }

  private def serializeReadCommand(readCmd: ReadCommand[String, String, ?], buffer: ByteBuffer): Unit = {
    readCmd match {
      case Get(key) =>
        buffer.put(1.toByte)
        serializeByteArray(key.value.getBytes("UTF-8"), buffer)
      case Scan(startKey, limit) =>
        buffer.put(2.toByte)
        serializeByteArray(startKey.value.getBytes("UTF-8"), buffer)
        buffer.putInt(limit): Unit
      case Range(startKey, endKey) =>
        buffer.put(3.toByte)
        serializeByteArray(startKey.value.getBytes("UTF-8"), buffer)
        serializeByteArray(endKey.value.getBytes("UTF-8"), buffer)
      case Keys(prefix) =>
        buffer.put(4.toByte)
        prefix match {
          case Some(p) =>
            buffer.put(1.toByte)
            serializeByteArray(p.value.getBytes("UTF-8"), buffer)
          case None =>
            buffer.put(0.toByte): Unit
        }
    }
  }

  private def deserializeReadCommand(buffer: ByteBuffer): ReadCommand[String, String, ?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val key = new String(deserializeByteArray(buffer), "UTF-8")
        Get(TypedValue(key, StringType))
      case 2 =>
        val startKey = new String(deserializeByteArray(buffer), "UTF-8")
        val limit    = buffer.getInt()
        Scan(TypedValue(startKey, StringType), limit)
      case 3 =>
        val startKey = new String(deserializeByteArray(buffer), "UTF-8")
        val endKey   = new String(deserializeByteArray(buffer), "UTF-8")
        Range(TypedValue(startKey, StringType), TypedValue(endKey, StringType))
      case 4 =>
        val hasPrefix = buffer.get()
        val prefix = if (hasPrefix == 1) {
          val prefixStr = new String(deserializeByteArray(buffer), "UTF-8")
          Some(TypedValue(prefixStr, StringType))
        } else None
        Keys(prefix)
    }
  }

  private def serializeByteArray(bytes: Array[Byte], buffer: ByteBuffer): Unit = {
    buffer.putInt(bytes.length)
    buffer.put(bytes): Unit
  }

  private def deserializeByteArray(buffer: ByteBuffer): Array[Byte] = {
    val length = buffer.getInt()
    val bytes  = new Array[Byte](length)
    buffer.get(bytes)
    bytes
  }
}
