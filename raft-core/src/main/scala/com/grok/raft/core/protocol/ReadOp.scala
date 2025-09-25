package com.grok.raft.core.protocol

// Typed key-value read operations using TypedValue for keys and values
case class Get[K, V](key: TypedValue[K]) extends ReadCommand[K, V, Option[V]]

case class Scan[K, V](startKey: TypedValue[K], limit: Int) extends ReadCommand[K, V, List[V]]

case class Range[K, V](startKey: TypedValue[K], endKey: TypedValue[K]) extends ReadCommand[K, V, List[V]]

case class Keys[K, V](prefix: Option[TypedValue[K]] = None) extends ReadCommand[K, V, List[K]]
