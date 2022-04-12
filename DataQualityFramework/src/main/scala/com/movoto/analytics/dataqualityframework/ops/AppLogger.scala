package com.movoto.analytics.dataqualityframework.ops

import org.apache.log4j.Logger

/**
 * Interface which enables logging feature for the application
 */
trait AppLogger extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

}
