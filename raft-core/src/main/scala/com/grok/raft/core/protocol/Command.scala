package com.grok.raft.core.protocol

sealed trait Command[T]

trait ReadCommand[T] extends Command[T]
trait WriteCommand[T] extends Command[T]
