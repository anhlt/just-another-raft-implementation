package com.grok.raft.core.protocol

sealed trait Command

trait ReadCommand[T] extends Command
trait WriteCommand[T] extends Command

// Basic KV Operations
case class Put(key: Array[Byte], value: Array[Byte]) extends WriteCommand[Unit]
case class Delete(key: Array[Byte]) extends WriteCommand[Unit]
case class Get(key: Array[Byte]) extends ReadCommand[Option[Array[Byte]]]
case class Contains(key: Array[Byte]) extends ReadCommand[Boolean]

// Range and Scan Operations  
case class Range(startKey: Array[Byte], endKey: Array[Byte], limit: Option[Int] = None) 
  extends ReadCommand[List[(Array[Byte], Array[Byte])]]

case class Scan(prefix: Array[Byte], limit: Option[Int] = None) 
  extends ReadCommand[List[(Array[Byte], Array[Byte])]]

case class Keys(prefix: Option[Array[Byte]] = None, limit: Option[Int] = None) 
  extends ReadCommand[List[Array[Byte]]]
