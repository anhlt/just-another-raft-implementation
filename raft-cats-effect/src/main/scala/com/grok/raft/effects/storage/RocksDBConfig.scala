package com.grok.raft.effects.storage

import org.rocksdb.*
import java.nio.file.Path
import java.util.concurrent.TimeUnit

case class RocksDBConfig(
    logDbPath: Path,
    stateMachineDbPath: Path,
    maxBackgroundJobs: Int = 4,
    maxLogFileSize: Long = 64L * 1024 * 1024, // 64MB
    keepLogFileNum: Long = 10,
    writeBufferSize: Long = 64L * 1024 * 1024, // 64MB
    maxWriteBufferNumber: Int = 3,
    compressionType: CompressionType = CompressionType.LZ4_COMPRESSION,
    enableStatistics: Boolean = true
):

  def createLogDBOptions(): DBOptions =
    val options = new DBOptions()
    options.setCreateIfMissing(true)
    options.setCreateMissingColumnFamilies(true)
    options.setMaxBackgroundJobs(maxBackgroundJobs)
    options.setMaxLogFileSize(maxLogFileSize)
    options.setKeepLogFileNum(keepLogFileNum)
    if enableStatistics then options.setStatistics(new Statistics()): Unit
    options

  def createStateMachineDBOptions(): DBOptions =
    val options = new DBOptions()
    options.setCreateIfMissing(true)
    options.setCreateMissingColumnFamilies(true)
    options.setMaxBackgroundJobs(maxBackgroundJobs)
    if enableStatistics then options.setStatistics(new Statistics()): Unit
    options

  def createColumnFamilyOptions(): ColumnFamilyOptions =
    val options = new ColumnFamilyOptions()
    options.setCompressionType(compressionType)
    options.setWriteBufferSize(writeBufferSize)
    options.setMaxWriteBufferNumber(maxWriteBufferNumber)
    options

object RocksDBConfig:
  def default(baseDir: Path): RocksDBConfig =
    RocksDBConfig(
      logDbPath = baseDir.resolve("raft-log"),
      stateMachineDbPath = baseDir.resolve("state-machine")
    )