package com.grok.raft.core


trait Deferred[F[_], A] :

  def get: F[A]

  def complete(a: A): F[Unit]