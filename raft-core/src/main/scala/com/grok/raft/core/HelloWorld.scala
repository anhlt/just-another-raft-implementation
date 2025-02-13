package com.grok.raft.core

import com.grok.raft.grpc.Raft

import cats.effect.IO

object HelloWorld {

  def say(): IO[String] = IO.delay("Hello Cats!")
}
