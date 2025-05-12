package com.grok.raft.effects.internal

import com.grok.raft.core.internal.Deferred 
import cats.effect.{IO, Deferred => EFDeferred}



class DeferredImpl[F[_], A](concurrentDeffered: EFDeferred[F, A]) extends Deferred[F, A]:


  override def complete(a: A): F[Boolean] = concurrentDeffered.complete(a)

  override def get: F[A] = concurrentDeffered.get

