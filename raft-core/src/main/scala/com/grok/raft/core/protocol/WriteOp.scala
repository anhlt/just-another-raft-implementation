package com.grok.raft.core.protocol

// Typed key-value write operations using TypedValue for keys and values
case class Create[K, V](key: TypedValue[K], value: TypedValue[V]) extends WriteCommand[K, V, Option[V]]

case class Update[K, V](key: TypedValue[K], value: TypedValue[V]) extends WriteCommand[K, V, Option[V]]

case class Delete[K, V](key: TypedValue[K]) extends WriteCommand[K, V, Option[V]]

case class Upsert[K, V](key: TypedValue[K], value: TypedValue[V]) extends WriteCommand[K, V, Option[V]]
