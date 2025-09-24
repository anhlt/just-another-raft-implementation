package com.grok.raft.core.protocol

sealed trait WriteOp[K, V] extends WriteCommand[Option[V]]

case class Create[K, V](key: K, value: V) extends WriteOp[K, V]

case class Update[K, V](key: K, value: V) extends WriteOp[K, V]

case class Delete[K, V](key: K) extends WriteOp[K, V]

case class Upsert[K, V](key: K, value: V) extends WriteOp[K, V]