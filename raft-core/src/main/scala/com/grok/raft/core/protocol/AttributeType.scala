package com.grok.raft.core.protocol

sealed trait AttributeType[T]

case object StringType extends AttributeType[String]
case object NumberType extends AttributeType[BigDecimal]
case object BinaryType extends AttributeType[Array[Byte]]
case object BooleanType extends AttributeType[Boolean]
case object NullType extends AttributeType[Unit]

case class MapType[V](valueType: AttributeType[V]) extends AttributeType[Map[String, V]]
case class ListType[T](elementType: AttributeType[T]) extends AttributeType[List[T]]

case class StringSetType() extends AttributeType[Set[String]]
case class NumberSetType() extends AttributeType[Set[BigDecimal]]  
case class BinarySetType() extends AttributeType[Set[Array[Byte]]]

case class TypedValue[T](value: T, attributeType: AttributeType[T])

object AttributeType {
  def typeDescriptor[T](attributeType: AttributeType[T]): String = attributeType match {
    case StringType => "S"
    case NumberType => "N"
    case BinaryType => "B"
    case BooleanType => "BOOL"
    case NullType => "NULL"
    case _: MapType[_] => "M"
    case _: ListType[_] => "L"
    case StringSetType() => "SS"
    case NumberSetType() => "NS"
    case BinarySetType() => "BS"
  }
  
  def fromDescriptor(descriptor: String): AttributeType[?] = descriptor match {
    case "S" => StringType
    case "N" => NumberType
    case "B" => BinaryType
    case "BOOL" => BooleanType
    case "NULL" => NullType
    case "M" => MapType(StringType)
    case "L" => ListType(StringType)
    case "SS" => StringSetType()
    case "NS" => NumberSetType()
    case "BS" => BinarySetType()
    case _ => throw new IllegalArgumentException(s"Unknown type descriptor: $descriptor")
  }
}