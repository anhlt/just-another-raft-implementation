package com.grok.raft.core.internal


trait RaftDeferred[F[_], A] :

  def get: F[A]

  def complete(a: A): F[Boolean]