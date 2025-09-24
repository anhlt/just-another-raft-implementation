package com.grok.raft.effects.storage

import com.grok.raft.core.storage.{BinarySerializable, DefaultBinarySerializer}
import com.grok.raft.core.internal.LogEntry

object BinarySerializer {
  def serializeLogEntry(entry: LogEntry): Array[Byte] = 
    DefaultBinarySerializer.serializeLogEntry(entry)
    
  def deserializeLogEntry(bytes: Array[Byte]): LogEntry = 
    DefaultBinarySerializer.deserializeLogEntry(bytes)
}