package polaris.kafka.model

import scala.collection.mutable

/**
  * Created by chengli at 15/12/2017
  */
class PartitionsState extends mutable.HashMap[String, Map[String, AnyRef]]{
  def getPartState(parId: String): Map[String, AnyRef] = this(parId)
}
