package com.grok.raft.core.protocol

sealed trait ReadOp[K, V] extends ReadCommand[Option[V]]

case class Get[K, V](key: K) extends ReadOp[K, V]

case class Scan[K, V](startKey: K, limit: Int) extends ReadOp[K, V]

case class Range[K, V](startKey: K, endKey: K) extends ReadOp[K, V]

case class Keys[K, V](prefix: Option[K] = None) extends ReadOp[K, V]
