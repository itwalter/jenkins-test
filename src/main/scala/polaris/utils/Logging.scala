package polaris.utils

import org.apache.log4j.Logger
/**
  * Created by chengli at 08/12/2017
  */
trait Logging {
  val log: Logger = Logger.getLogger(this.getClass.getName)
}
