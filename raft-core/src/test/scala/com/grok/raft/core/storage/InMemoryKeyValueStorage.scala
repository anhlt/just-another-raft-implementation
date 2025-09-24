package com.grok.raft.core.storage

import cats.effect.Sync
import cats.effect.Ref
import cats.syntax.all.*
import scala.collection.immutable.Map

class InMemoryKeyValueStorage[F[_]: Sync] private (
    data: Ref[F, Map[String, String]]
) extends KeyValueStorage[F] {

  override def get(key: String): F[Option[String]] =
    data.get.map(_.get(key))

  override def put(key: String, value: String): F[Option[String]] =
    data.modify { m =>
      val oldValue = m.get(key)
      (m + (key -> value), oldValue)
    }

  override def remove(key: String): F[Option[String]] =
    data.modify { m =>
      val oldValue = m.get(key)
      (m - key, oldValue)
    }

  override def scan(prefix: String): F[Map[String, String]] =
    data.get.map(_.filter((k, _) => k.startsWith(prefix)))

  override def keys(): F[Set[String]] =
    data.get.map(_.keySet)

  override def range(startKey: String, endKey: String): F[Map[String, String]] =
    data.get.map(_.filter { case (k, _) =>
      k >= startKey && k < endKey
    })

  override def close(): F[Unit] =
    Sync[F].unit
}

object InMemoryKeyValueStorage {
  def apply[F[_]: Sync](): F[InMemoryKeyValueStorage[F]] =
    Ref.of[F, Map[String, String]](Map.empty).map(new InMemoryKeyValueStorage[F](_))
}
