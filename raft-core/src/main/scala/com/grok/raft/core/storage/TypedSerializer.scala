package com.grok.raft.core.storage

import com.grok.raft.core.protocol.{AttributeType, TypedValue, StringType, NumberType, BinaryType, BooleanType, NullType}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait TypedSerializer[T] {
  def attributeType: AttributeType[T]
  def serialize(value: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
  def serializeTyped(typedValue: TypedValue[T]): Array[Byte]
  def deserializeTyped(bytes: Array[Byte]): TypedValue[T]
}

object TypedSerializer {
  
  def forType[T](attrType: AttributeType[T]): TypedSerializer[T] = attrType match {
    case StringType => StringSerializer().asInstanceOf[TypedSerializer[T]]
    case NumberType => NumberSerializer().asInstanceOf[TypedSerializer[T]]
    case BinaryType => BinarySerializer().asInstanceOf[TypedSerializer[T]]
    case BooleanType => BooleanSerializer().asInstanceOf[TypedSerializer[T]]
    case NullType => NullSerializer().asInstanceOf[TypedSerializer[T]]
    case _ => throw new IllegalArgumentException(s"Unsupported type: $attrType")
  }
}

class StringSerializer extends TypedSerializer[String] {
  override def attributeType: AttributeType[String] = StringType
  
  override def serialize(value: String): Array[Byte] = 
    value.getBytes(StandardCharsets.UTF_8)
    
  override def deserialize(bytes: Array[Byte]): String = 
    new String(bytes, StandardCharsets.UTF_8)
    
  override def serializeTyped(typedValue: TypedValue[String]): Array[Byte] = {
    val valueBytes = serialize(typedValue.value)
    val typeDesc = AttributeType.typeDescriptor(typedValue.attributeType).getBytes(StandardCharsets.UTF_8)
    
    val buffer = ByteBuffer.allocate(1 + typeDesc.length + 4 + valueBytes.length)
    buffer.put(typeDesc.length.toByte)
    buffer.put(typeDesc)
    buffer.putInt(valueBytes.length)
    buffer.put(valueBytes)
    buffer.array()
  }
  
  override def deserializeTyped(bytes: Array[Byte]): TypedValue[String] = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeDescLength = buffer.get().toInt
    val typeDescBytes = new Array[Byte](typeDescLength)
    buffer.get(typeDescBytes)
    val valueLength = buffer.getInt()
    val valueBytes = new Array[Byte](valueLength)
    buffer.get(valueBytes)
    
    TypedValue(deserialize(valueBytes), attributeType)
  }
}

class NumberSerializer extends TypedSerializer[BigDecimal] {
  override def attributeType: AttributeType[BigDecimal] = NumberType
  
  override def serialize(value: BigDecimal): Array[Byte] = 
    value.toString.getBytes(StandardCharsets.UTF_8)
    
  override def deserialize(bytes: Array[Byte]): BigDecimal = 
    BigDecimal(new String(bytes, StandardCharsets.UTF_8))
    
  override def serializeTyped(typedValue: TypedValue[BigDecimal]): Array[Byte] = {
    val valueBytes = serialize(typedValue.value)
    val typeDesc = AttributeType.typeDescriptor(typedValue.attributeType).getBytes(StandardCharsets.UTF_8)
    
    val buffer = ByteBuffer.allocate(1 + typeDesc.length + 4 + valueBytes.length)
    buffer.put(typeDesc.length.toByte)
    buffer.put(typeDesc)
    buffer.putInt(valueBytes.length)
    buffer.put(valueBytes)
    buffer.array()
  }
  
  override def deserializeTyped(bytes: Array[Byte]): TypedValue[BigDecimal] = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeDescLength = buffer.get().toInt
    val typeDescBytes = new Array[Byte](typeDescLength)
    buffer.get(typeDescBytes)
    val valueLength = buffer.getInt()
    val valueBytes = new Array[Byte](valueLength)
    buffer.get(valueBytes)
    
    TypedValue(deserialize(valueBytes), attributeType)
  }
}

class BinarySerializer extends TypedSerializer[Array[Byte]] {
  override def attributeType: AttributeType[Array[Byte]] = BinaryType
  
  override def serialize(value: Array[Byte]): Array[Byte] = value
  
  override def deserialize(bytes: Array[Byte]): Array[Byte] = bytes
  
  override def serializeTyped(typedValue: TypedValue[Array[Byte]]): Array[Byte] = {
    val valueBytes = serialize(typedValue.value)
    val typeDesc = AttributeType.typeDescriptor(typedValue.attributeType).getBytes(StandardCharsets.UTF_8)
    
    val buffer = ByteBuffer.allocate(1 + typeDesc.length + 4 + valueBytes.length)
    buffer.put(typeDesc.length.toByte)
    buffer.put(typeDesc)
    buffer.putInt(valueBytes.length)
    buffer.put(valueBytes)
    buffer.array()
  }
  
  override def deserializeTyped(bytes: Array[Byte]): TypedValue[Array[Byte]] = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeDescLength = buffer.get().toInt
    val typeDescBytes = new Array[Byte](typeDescLength)
    buffer.get(typeDescBytes)
    val valueLength = buffer.getInt()
    val valueBytes = new Array[Byte](valueLength)
    buffer.get(valueBytes)
    
    TypedValue(deserialize(valueBytes), attributeType)
  }
}

class BooleanSerializer extends TypedSerializer[Boolean] {
  override def attributeType: AttributeType[Boolean] = BooleanType
  
  override def serialize(value: Boolean): Array[Byte] = 
    Array(if (value) 1.toByte else 0.toByte)
    
  override def deserialize(bytes: Array[Byte]): Boolean = 
    bytes(0) != 0
    
  override def serializeTyped(typedValue: TypedValue[Boolean]): Array[Byte] = {
    val valueBytes = serialize(typedValue.value)
    val typeDesc = AttributeType.typeDescriptor(typedValue.attributeType).getBytes(StandardCharsets.UTF_8)
    
    val buffer = ByteBuffer.allocate(1 + typeDesc.length + 4 + valueBytes.length)
    buffer.put(typeDesc.length.toByte)
    buffer.put(typeDesc)
    buffer.putInt(valueBytes.length)
    buffer.put(valueBytes)
    buffer.array()
  }
  
  override def deserializeTyped(bytes: Array[Byte]): TypedValue[Boolean] = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeDescLength = buffer.get().toInt
    val typeDescBytes = new Array[Byte](typeDescLength)
    buffer.get(typeDescBytes)
    val valueLength = buffer.getInt()
    val valueBytes = new Array[Byte](valueLength)
    buffer.get(valueBytes)
    
    TypedValue(deserialize(valueBytes), attributeType)
  }
}

class NullSerializer extends TypedSerializer[Unit] {
  override def attributeType: AttributeType[Unit] = NullType
  
  override def serialize(value: Unit): Array[Byte] = Array.empty[Byte]
  
  override def deserialize(bytes: Array[Byte]): Unit = ()
  
  override def serializeTyped(typedValue: TypedValue[Unit]): Array[Byte] = {
    val typeDesc = AttributeType.typeDescriptor(typedValue.attributeType).getBytes(StandardCharsets.UTF_8)
    
    val buffer = ByteBuffer.allocate(1 + typeDesc.length + 4)
    buffer.put(typeDesc.length.toByte)
    buffer.put(typeDesc)
    buffer.putInt(0)
    buffer.array()
  }
  
  override def deserializeTyped(bytes: Array[Byte]): TypedValue[Unit] = {
    TypedValue((), attributeType)
  }
}