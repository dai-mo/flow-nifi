package org.dcs.iot.kaa

import org.kaaproject.kaa.common.dto.logs.LogAppenderDto
import org.kaaproject.kaa.server.common.log.shared.appender.{AbstractLogAppender, LogDeliveryCallback, LogEventPack}
import org.kaaproject.kaa.server.common.log.shared.avro.gen.RecordHeader

/**
  * [[https://kaaproject.github.io/kaa/docs/v0.10.0/Customization-guide/Log-appenders/ Custom log appender]]
  * class for the Kaa IoT Platform.
  *
  * This class uses the Nifi
  * [[https://docs.hortonworks.com/HDPDocuments/HDF3/HDF-3.0.0/bk_user-guide/content/site-to-site.html Site-to-Site (S2S)]]
  * protocol over sockets to send data from the Kaa IoT Platform to the configured Nifi Input Ports.
  *
  * This is achieved using the Nifi S2S Client.
  *
  * @author cmathew
  * @constructor
  */
class NifiS2SAppender(configurationClass: Class[NifiS2SConfiguration] ) extends AbstractLogAppender(configurationClass) {

  /**
    *
    * @param logEventPack
    * @param header
    * @param listener
    */
  override def doAppend(logEventPack: LogEventPack, header: RecordHeader, listener: LogDeliveryCallback): Unit = ???

  /**
    *
    * @param appender
    * @param configuration
    */
  override def initFromConfiguration(appender: LogAppenderDto, configuration: NifiS2SConfiguration): Unit = ???

  /**
    *
    */
  override def close(): Unit = ???
}
