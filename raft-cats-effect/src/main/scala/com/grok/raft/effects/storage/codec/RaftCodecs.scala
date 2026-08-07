package com.grok.raft.effects.storage.codec

import scodec.{Codec, Err}
import scodec.codecs.*
import scodec.bits.{ByteVector, BitVector}
import com.grok.raft.core.internal.{LogEntry, NodeAddress}
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.PersistedState
import cats.syntax.either.*

object RaftCodecs:

  // Helper for variable-length byte arrays
  private val bytes32: Codec[Array[Byte]] = variableSizeBytes(int32, bytes).xmap(_.toArray, ByteVector.apply)

  implicit val nodeAddressCodec: Codec[NodeAddress] = 
    (utf8_32 :: int32).as[NodeAddress]

  implicit val commandCodec: Codec[Command] = 
    discriminated[Command].by(uint8)
      .typecase(1, (bytes32 :: bytes32).as[Put])
      .typecase(2, bytes32.as[Delete]) 
      .typecase(3, bytes32.as[Get])
      .typecase(4, bytes32.as[Contains])
      .typecase(5, (bytes32 :: bytes32 :: optional(bool, int32)).as[Range])
      .typecase(6, (bytes32 :: optional(bool, int32)).as[Scan])
      .typecase(7, (optional(bool, bytes32) :: optional(bool, int32)).as[Keys])

  implicit val logEntryCodec: Codec[LogEntry] =
    (int64 :: int64 :: commandCodec).as[LogEntry]

  implicit val persistedStateCodec: Codec[PersistedState] =
    (int64 :: optional(bool, nodeAddressCodec) :: int64).as[PersistedState]

  def encodeLogEntry(entry: LogEntry): Either[Err, Array[Byte]] =
    logEntryCodec.encode(entry).map(_.toByteArray).toEither

  def decodeLogEntry(bytes: Array[Byte]): Either[Err, LogEntry] =
    logEntryCodec.decode(BitVector(bytes)).map(_.value).toEither

  def encodePersistedState(state: PersistedState): Either[Err, Array[Byte]] =
    persistedStateCodec.encode(state).map(_.toByteArray).toEither

  def decodePersistedState(bytes: Array[Byte]): Either[Err, PersistedState] =
    persistedStateCodec.decode(BitVector(bytes)).map(_.value).toEither