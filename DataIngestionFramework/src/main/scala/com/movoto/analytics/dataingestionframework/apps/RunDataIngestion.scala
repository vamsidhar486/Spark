package com.movoto.analytics.dataingestionframework.apps

import com.movoto.analytics.dataingestionframework.ops.SparkOps.config
import com.movoto.analytics.dataingestionframework.services.MiscUtils

object RunDataIngestion extends App {
  MiscUtils.runDataIngestion(config.SOURCE,config.TARGET)

}
