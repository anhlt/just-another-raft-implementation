package com.grok.raft.core.storage

import scala.collection.immutable.Map

trait KeyValueStorage[F[_]] {
  def get(key: String): F[Option[String]]
  def put(key: String, value: String): F[Option[String]]
  def remove(key: String): F[Option[String]]
  def scan(prefix: String): F[Map[String, String]]
  def keys(): F[Set[String]]
  def range(startKey: String, endKey: String): F[Map[String, String]]
  def close(): F[Unit]
}
