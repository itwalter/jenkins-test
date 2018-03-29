package polaris.utils

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._

/**
  * Created by chengli at 07/12/2017
  */
object JsonUtils {
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def toJson(value: java.util.Map[String, AnyRef]): String = {
    toJson(value.asScala)
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toBytes(value: Any): Array[Byte] = {
    mapper.writeValueAsBytes(value)
  }

  def toScalaMap(value: String): Map[String, AnyRef] = {
    mapper.readValue(value, classOf[Map[String, AnyRef]])
  }

  def toJavaMap(value: Any): java.util.Map[String, Object] = {
    mapper.convertValue(value, classOf[java.util.Map[String, Object]])
  }

  def toJavaMap(value: String): java.util.HashMap[String, Object] = {
    mapper.readValue(value, new TypeReference[java.util.Map[String, Object]](){})
  }

  def fromMap[T](value: java.util.Map[String, Any], clazz: Class[T]): T = {
    mapper.convertValue(value.asScala, clazz)
  }

  def fromJson[T](value: String, clazz: Class[T]): T = {
    mapper.readValue(value, clazz)
  }
}
