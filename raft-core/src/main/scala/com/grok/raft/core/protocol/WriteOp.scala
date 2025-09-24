package com.grok.raft.core.protocol

// Key-Value write operations using Array[Byte] for keys and values
case class Create(key: Array[Byte], value: Array[Byte]) extends WriteCommand[Option[Array[Byte]]]

case class Update(key: Array[Byte], value: Array[Byte]) extends WriteCommand[Option[Array[Byte]]]

case class Delete(key: Array[Byte]) extends WriteCommand[Option[Array[Byte]]]

case class Upsert(key: Array[Byte], value: Array[Byte]) extends WriteCommand[Option[Array[Byte]]]
