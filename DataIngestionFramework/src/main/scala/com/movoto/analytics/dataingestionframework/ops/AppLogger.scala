package com.movoto.analytics.dataingestionframework.ops

import org.apache.log4j.Logger

trait AppLogger extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

}
