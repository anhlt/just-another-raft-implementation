package com.grok.raft.core.protocol

sealed trait Command[K, V, T]

trait ReadCommand[K, V, T]  extends Command[K, V, T]
trait WriteCommand[K, V, T] extends Command[K, V, T]
