package com.grok.raft.core.storage

import com.grok.raft.core.protocol.{
  Command,
  ReadCommand,
  WriteCommand,
  Get,
  Scan,
  Range,
  Keys,
  Create,
  Update,
  Delete,
  Upsert,
  TypedValue
}
import java.nio.ByteBuffer

trait CommandSerializer[K, V] {
  def serializeCommand(command: Command[K, V, ?]): Array[Byte]
  def deserializeCommand(bytes: Array[Byte]): Command[K, V, ?]
}

class TypedCommandSerializer[K, V](
    keySerializer: TypedSerializer[K],
    valueSerializer: TypedSerializer[V]
) extends CommandSerializer[K, V] {

  override def serializeCommand(command: Command[K, V, ?]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4096)
    
    command match {
      case cmd: ReadCommand[K, V, ?] =>
        buffer.put(1.toByte) // ReadCommand marker
        serializeReadCommand(cmd, buffer)
        
      case cmd: WriteCommand[K, V, ?] =>
        buffer.put(2.toByte) // WriteCommand marker  
        serializeWriteCommand(cmd, buffer)
    }
    
    val result = new Array[Byte](buffer.position())
    buffer.flip()
    buffer.get(result)
    result
  }

  override def deserializeCommand(bytes: Array[Byte]): Command[K, V, ?] = {
    val buffer = ByteBuffer.wrap(bytes)
    val commandType = buffer.get()
    
    commandType match {
      case 1 => deserializeReadCommand(buffer)
      case 2 => deserializeWriteCommand(buffer)
      case _ => throw new IllegalArgumentException(s"Unknown command type: $commandType")
    }
  }

  private def serializeReadCommand(cmd: ReadCommand[K, V, ?], buffer: ByteBuffer): Unit = {
    cmd match {
      case Get(key) =>
        buffer.put(1.toByte)
        val keyBytes = keySerializer.serializeTyped(key)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        
      case Scan(startKey, limit) =>
        buffer.put(2.toByte)
        val keyBytes = keySerializer.serializeTyped(startKey)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        buffer.putInt(limit)
        
      case Range(startKey, endKey) =>
        buffer.put(3.toByte)
        val startKeyBytes = keySerializer.serializeTyped(startKey)
        buffer.putInt(startKeyBytes.length)
        buffer.put(startKeyBytes)
        val endKeyBytes = keySerializer.serializeTyped(endKey)
        buffer.putInt(endKeyBytes.length)
        buffer.put(endKeyBytes)
        
      case Keys(prefix) =>
        buffer.put(4.toByte)
        prefix match {
          case Some(p) =>
            buffer.put(1.toByte)
            val prefixBytes = keySerializer.serializeTyped(p)
            buffer.putInt(prefixBytes.length)
            buffer.put(prefixBytes)
          case None =>
            buffer.put(0.toByte)
        }
    }
  }

  private def deserializeReadCommand(buffer: ByteBuffer): ReadCommand[K, V, ?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = keySerializer.deserializeTyped(keyBytes)
        Get(key)
        
      case 2 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val startKey = keySerializer.deserializeTyped(keyBytes)
        val limit = buffer.getInt()
        Scan(startKey, limit)
        
      case 3 =>
        val startKeyLength = buffer.getInt()
        val startKeyBytes = new Array[Byte](startKeyLength)
        buffer.get(startKeyBytes)
        val startKey = keySerializer.deserializeTyped(startKeyBytes)
        
        val endKeyLength = buffer.getInt()
        val endKeyBytes = new Array[Byte](endKeyLength)
        buffer.get(endKeyBytes)
        val endKey = keySerializer.deserializeTyped(endKeyBytes)
        
        Range(startKey, endKey)
        
      case 4 =>
        val hasPrefix = buffer.get()
        val prefix = if (hasPrefix == 1) {
          val prefixLength = buffer.getInt()
          val prefixBytes = new Array[Byte](prefixLength)
          buffer.get(prefixBytes)
          Some(keySerializer.deserializeTyped(prefixBytes))
        } else None
        Keys(prefix)
        
      case _ => throw new IllegalArgumentException(s"Unknown read command type: $opType")
    }
  }

  private def serializeWriteCommand(cmd: WriteCommand[K, V, ?], buffer: ByteBuffer): Unit = {
    cmd match {
      case Create(key, value) =>
        buffer.put(1.toByte)
        val keyBytes = keySerializer.serializeTyped(key)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        val valueBytes = valueSerializer.serializeTyped(value)
        buffer.putInt(valueBytes.length)
        buffer.put(valueBytes)
        
      case Update(key, value) =>
        buffer.put(2.toByte)
        val keyBytes = keySerializer.serializeTyped(key)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        val valueBytes = valueSerializer.serializeTyped(value)
        buffer.putInt(valueBytes.length)
        buffer.put(valueBytes)
        
      case Delete(key) =>
        buffer.put(3.toByte)
        val keyBytes = keySerializer.serializeTyped(key)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        
      case Upsert(key, value) =>
        buffer.put(4.toByte)
        val keyBytes = keySerializer.serializeTyped(key)
        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)
        val valueBytes = valueSerializer.serializeTyped(value)
        buffer.putInt(valueBytes.length)
        buffer.put(valueBytes)
    }
  }

  private def deserializeWriteCommand(buffer: ByteBuffer): WriteCommand[K, V, ?] = {
    val opType = buffer.get()
    opType match {
      case 1 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = keySerializer.deserializeTyped(keyBytes)
        
        val valueLength = buffer.getInt()
        val valueBytes = new Array[Byte](valueLength)
        buffer.get(valueBytes)
        val value = valueSerializer.deserializeTyped(valueBytes)
        
        Create(key, value)
        
      case 2 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = keySerializer.deserializeTyped(keyBytes)
        
        val valueLength = buffer.getInt()
        val valueBytes = new Array[Byte](valueLength)
        buffer.get(valueBytes)
        val value = valueSerializer.deserializeTyped(valueBytes)
        
        Update(key, value)
        
      case 3 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = keySerializer.deserializeTyped(keyBytes)
        Delete(key)
        
      case 4 =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = keySerializer.deserializeTyped(keyBytes)
        
        val valueLength = buffer.getInt()
        val valueBytes = new Array[Byte](valueLength)
        buffer.get(valueBytes)
        val value = valueSerializer.deserializeTyped(valueBytes)
        
        Upsert(key, value)
        
      case _ => throw new IllegalArgumentException(s"Unknown write command type: $opType")
    }
  }
}