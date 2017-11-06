package org.dcs.iot.kaa

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.text.MessageFormat
import java.util

import org.apache.nifi.remote.TransferDirection
import org.apache.nifi.remote.client.SiteToSiteClient
import org.apache.nifi.remote.util.StandardDataPacket
import org.kaaproject.kaa.common.dto.logs.LogAppenderDto
import org.kaaproject.kaa.server.common.log.shared.appender.{AbstractLogAppender, LogDeliveryCallback, LogEventPack}
import org.kaaproject.kaa.server.common.log.shared.avro.gen.RecordHeader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

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
class NifiS2SAppender extends AbstractLogAppender(classOf[NifiS2SConfiguration]) {

  val LOG: Logger = LoggerFactory.getLogger(classOf[NifiS2SAppender])

  var client: SiteToSiteClient = _

  def send(record: String): Unit = {
    val transaction = client.createTransaction(TransferDirection.SEND)
    if (transaction == null) throw new IllegalStateException("Unable to create a NiFi Transaction to send data")
    val data = record.getBytes(StandardCharsets.UTF_8)
    val bais = new ByteArrayInputStream(data)
    val packet = new StandardDataPacket(new util.HashMap(), bais, data.length)
    transaction.send(data, new util.HashMap())
    transaction.confirm()
    transaction.complete()
  }

  /**
    *
    * @param logEventPack
    * @param header
    * @param listener
    */
  override def doAppend(logEventPack: LogEventPack, header: RecordHeader, listener: LogDeliveryCallback): Unit = {
    try {
      val logEvents = generateLogEvent(logEventPack, header)
      logEvents.asScala.foreach { le =>
        send(le.getEvent)
      }
      LOG.debug("Sending {} records to [{}]",
        logEventPack.getEvents.size(),
        "url:" + client.getConfig.getUrl + ",inputPortId:" + client.getConfig.getPortIdentifier)
      listener.onSuccess()
    } catch {
      case NonFatal(e) => {
        LOG.error(MessageFormat.format("Failed to send records to [{0}]",
          "url:" + client.getConfig.getUrl + ",inputPortId:" + client.getConfig.getPortIdentifier,
          e))
        listener.onInternalError()
      }
    }
  }

  /**
    *
    * @param appender
    * @param configuration
    */
  override def initFromConfiguration(appender: LogAppenderDto, configuration: NifiS2SConfiguration): Unit = {
    LOG.debug("Initialising Nifi Site-to-Site Client")
    client = new SiteToSiteClient.Builder()
      .url(configuration.getBaseUrl.toString + ":" + configuration.getPort.toString + "/nifi")
      .portIdentifier(configuration.getInputPortName.toString)
      .requestBatchCount(5)
      .build
  }

  /**
    *
    */
  override def close(): Unit = {
    client.close()
  }
}
