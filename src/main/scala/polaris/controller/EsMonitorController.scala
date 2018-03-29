package polaris.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, PathVariable, RequestMapping}
import polaris.elasticsearch.EsStat

/**
  * Created by chengli at 16/12/2017
  */
@Controller
@RequestMapping(path = Array("/monitor/es"), produces = Array("application/json"))
class EsMonitorController {
  @Autowired var esStat: EsStat = _

  @GetMapping(path = Array(""))
  def getClusterState = httpTemplate {
    esStat.clusterStatus
  }

  @GetMapping(path = Array("/nodes"))
  def getNodeState = httpTemplate {
    esStat.nodeStatus
  }

  @GetMapping(path = Array("/sources"))
  def getSources = httpTemplate {
    esStat.templateState
  }

  @GetMapping(path = Array("/sources/{source}"))
  def getSourceStat(@PathVariable("source") source: String) = httpTemplate {
    esStat.sourceStat(source)
  }
}
