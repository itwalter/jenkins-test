package polaris.utils

/**
  * Created by chengli at 14/12/2017
  */
trait JsonSerialization {
  def toJson: String = JsonUtils.toJson(this)
}

