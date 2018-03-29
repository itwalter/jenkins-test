package polaris.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, PathVariable, RequestMapping}
import polaris.kafka.KafkaStat

/**
  * Created by chengli at 15/12/2017
  */
@Controller
@RequestMapping(path = Array("/monitor/kafka"), produces = Array("application/json"))
class KafkaMonitorController {
  @Autowired
  private var kafkaStat: KafkaStat = _

  @GetMapping(path = Array("/nodes"))
  def getBrokers = httpTemplate {
    kafkaStat.getBrokers
  }

  @GetMapping(path = Array("/nodes/{broker}"))
  def getBroker(@PathVariable("broker") broker: String) = httpTemplate {
    kafkaStat.getBrokerInfo(broker)
  }

  @GetMapping(path = Array("/topics"))
  def getTopics = httpTemplate {
    kafkaStat.getTopics
  }

  @GetMapping(path = Array("/topics/{topic}"))
  def getTopicMetric(@PathVariable("topic") topic: String) = httpTemplate {
    kafkaStat.getTopicMetric(topic)
  }
}
