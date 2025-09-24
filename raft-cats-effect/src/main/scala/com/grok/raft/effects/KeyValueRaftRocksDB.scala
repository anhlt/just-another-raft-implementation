package com.grok.raft.effects

import com.grok.raft.core.{ClusterConfiguration, KeyValueRaft, MembershipManager, RpcClient}
import com.grok.raft.effects.storage.RocksDBKeyValueStorage
import cats.effect.Async
import cats.implicits.*
import org.typelevel.log4cats.Logger

/**
 * Factory for creating KeyValueRaft instances backed by RocksDB storage.
 */
object KeyValueRaftRocksDB {

  /**
   * Creates a new KeyValueRaft instance with RocksDB storage.
   * 
   * @param config The cluster configuration
   * @param dbPath Path to the RocksDB database directory
   * @param membershipManager The membership manager
   * @param rpcClient The RPC client
   * @return A KeyValueRaft instance backed by RocksDB
   */
  def create[F[_]: Async: Logger](
    config: ClusterConfiguration,
    dbPath: String,
    membershipManager: MembershipManager[F],
    rpcClient: RpcClient[F]
  ): F[KeyValueRaft[F]] = {
    for {
      // Create RocksDB storage
      storage <- RocksDBKeyValueStorage.create[F](dbPath)
      
      // Create KeyValueRaft with RocksDB storage
      kvRaft <- KeyValueRaft.create[F](config, storage, membershipManager, rpcClient)
    } yield kvRaft
  }

  /**
   * Creates a new KeyValueRaft instance with RocksDB storage and resource management.
   * The storage will be automatically closed when the resource is released.
   * 
   * @param config The cluster configuration
   * @param dbPath Path to the RocksDB database directory
   * @param membershipManager The membership manager
   * @param rpcClient The RPC client
   * @return A Resource containing a KeyValueRaft instance backed by RocksDB
   */
  def createResource[F[_]: Async: Logger](
    config: ClusterConfiguration,
    dbPath: String,
    membershipManager: MembershipManager[F],
    rpcClient: RpcClient[F]
  ): cats.effect.Resource[F, KeyValueRaft[F]] = {
    cats.effect.Resource.make(
      create[F](config, dbPath, membershipManager, rpcClient)
    )(_.close())
  }
}