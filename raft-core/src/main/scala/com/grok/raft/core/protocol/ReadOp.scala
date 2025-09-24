package com.grok.raft.core.protocol

// Key-Value read operations using Array[Byte] for keys and values
case class Get(key: Array[Byte]) extends ReadCommand[Option[Array[Byte]]]

case class Scan(startKey: Array[Byte], limit: Int) extends ReadCommand[Option[Array[Byte]]]

case class Range(startKey: Array[Byte], endKey: Array[Byte]) extends ReadCommand[List[Array[Byte]]]

case class Keys(prefix: Option[Array[Byte]] = None) extends ReadCommand[List[Array[Byte]]]
