package com.grok.raft.core.storage

import java.nio.ByteBuffer
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
  Keys
}
import com.grok.raft.core.internal.LogEntry

object ByteBufferBinarySerializer extends BinarySerializable {

  def serializeLogEntry(entry: LogEntry): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    buffer.putLong(entry.term)
    buffer.putLong(entry.index)

    entry.command match {
      case writeCmd: WriteCommand[?] =>
        buffer.put(1.toByte) // WriteCommand marker
        serializeWriteCommand(writeCmd, buffer)
      case readCmd: ReadCommand[?] =>
        buffer.put(2.toByte) // ReadCommand marker  
        serializeReadCommand(readCmd, buffer)
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
      case 1 => deserializeWriteCommand(buffer)
      case 2 => deserializeReadCommand(buffer)
      case _ =>
        val length       = buffer.getInt()
        val commandBytes = new Array[Byte](length)
        buffer.get(commandBytes)
        // For unknown commands, return a simple Get operation as fallback
        Get(commandBytes)
    }

    LogEntry(term, index, command)
  }

  private def serializeWriteCommand(writeCmd: WriteCommand[?], buffer: ByteBuffer): Unit = {
    writeCmd match {
      case Create(key, value) =>
        buffer.put(1.toByte)
        serializeByteArray(key, buffer)
        serializeByteArray(value, buffer)
      case Update(key, value) =>
        buffer.put(2.toByte)
        serializeByteArray(key, buffer)
        serializeByteArray(value, buffer)
      case Delete(key) =>
        buffer.put(3.toByte)
        serializeByteArray(key, buffer)
      case Upsert(key, value) =>
        buffer.put(4.toByte)
        serializeByteArray(key, buffer)
        serializeByteArray(value, buffer)
    }
  }

  private def deserializeWriteCommand(buffer: ByteBuffer): WriteCommand[?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val key = deserializeByteArray(buffer)
        val value = deserializeByteArray(buffer)
        Create(key, value)
      case 2 =>
        val key = deserializeByteArray(buffer)
        val value = deserializeByteArray(buffer)
        Update(key, value)
      case 3 =>
        val key = deserializeByteArray(buffer)
        Delete(key)
      case 4 =>
        val key = deserializeByteArray(buffer)
        val value = deserializeByteArray(buffer)
        Upsert(key, value)
    }
  }

  private def serializeReadCommand(readCmd: ReadCommand[?], buffer: ByteBuffer): Unit = {
    readCmd match {
      case Get(key) =>
        buffer.put(1.toByte)
        serializeByteArray(key, buffer)
      case Scan(startKey, limit) =>
        buffer.put(2.toByte)
        serializeByteArray(startKey, buffer)
         buffer.putInt(limit): Unit
      case Range(startKey, endKey) =>
        buffer.put(3.toByte)
        serializeByteArray(startKey, buffer)
        serializeByteArray(endKey, buffer)
      case Keys(prefix) =>
        buffer.put(4.toByte)
        prefix match {
          case Some(p) =>
            buffer.put(1.toByte)
            serializeByteArray(p, buffer)
          case None =>
             buffer.put(0.toByte): Unit
        }
    }
  }

  private def deserializeReadCommand(buffer: ByteBuffer): ReadCommand[?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val key = deserializeByteArray(buffer)
        Get(key)
      case 2 =>
        val startKey = deserializeByteArray(buffer)
        val limit    = buffer.getInt()
        Scan(startKey, limit)
      case 3 =>
        val startKey = deserializeByteArray(buffer)
        val endKey   = deserializeByteArray(buffer)
        Range(startKey, endKey)
      case 4 =>
        val hasPrefix = buffer.get()
        val prefix    = if (hasPrefix == 1) Some(deserializeByteArray(buffer)) else None
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