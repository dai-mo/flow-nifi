package org.dcs.iot.kaa.client

import java.io.IOException

import org.kaaproject.kaa.client.{DesktopKaaPlatformContext, Kaa, KaaClient, SimpleKaaClientStateListener}
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.dcs.iot.kaa.schema.config.HeartbeatConfiguration
import org.dcs.iot.kaa.schema.log.HeartbeatLog
import org.kaaproject.kaa.client.configuration.base.{ConfigurationListener, SimpleConfigurationStorage}
import org.kaaproject.kaa.client.logging.strategies.RecordCountLogUploadStrategy
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  *
  * @author cmathew
  * @constructor
  */
object HeartbeatMonitorClient {

  val LOG = LoggerFactory.getLogger(classOf[HeartbeatMonitorClient])

  val DEFAULT_START_DELAY = 1000L

  var kaaClient: KaaClient = _

  var scheduledFuture: ScheduledFuture[_] = _
  var scheduledExecutorService: ScheduledExecutorService = _

  def heartbeatRand(): Int = {
    new Random().nextInt(20)*2 - 20 + 80
  }

  def onKaaStarted(time: Long) {
    if (time <= 0) {
      LOG.error("Wrong time is used. Please, check your configuration!")
      kaaClient.stop()
      System.exit(0)
    }

    scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
      new Runnable() {
        @Override
        def run() {
          val heartbeat = heartbeatRand()
          kaaClient.addLogRecord(new HeartbeatLog(heartbeat.toLong))
          LOG.info("Sampled Heartbeat: {}", heartbeat)
        }
      }, 0, time, TimeUnit.MILLISECONDS)
  }

  def onChangedConfiguration(t: Long) {
    var time: Long = t
    if (time == 0) {
      time = DEFAULT_START_DELAY
    }
    scheduledFuture.cancel(false)

    scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
      new Runnable() {
        @Override
        def run() {
          val heartbeat = heartbeatRand()
          kaaClient.addLogRecord(new HeartbeatLog(heartbeat.toLong))
          LOG.info("Sampled Heartbeat: {}", heartbeat)
        }
      }, 0, time, TimeUnit.MILLISECONDS)
  }
  
  class HeartbeatMonitorClientStateListener extends SimpleKaaClientStateListener {

    override def onStarted() {
      super.onStarted()
      LOG.info("Kaa client started")
      val configuration: HeartbeatConfiguration  = kaaClient.getConfiguration()
      LOG.info("Default sample period: {}", configuration.getSamplePeriod())
      onKaaStarted(TimeUnit.SECONDS.toMillis(configuration.getSamplePeriod().toLong))
    }

    override def onStopped() {
      super.onStopped()
      LOG.info("Kaa client stopped")
    }
  }

  def main(args: Array[String]) {
    LOG.info(classOf[HeartbeatMonitorClient].getSimpleName + " app starting!")

    scheduledExecutorService = Executors.newScheduledThreadPool(1)

    //Create the Kaa desktop context for the application.
    val desktopKaaPlatformContext: DesktopKaaPlatformContext  = new DesktopKaaPlatformContext()

    /*
     * Create a Kaa client and add a listener which displays the Kaa client
     * configuration as soon as the Kaa client is started.
     */
    kaaClient = Kaa.newClient(desktopKaaPlatformContext, new HeartbeatMonitorClientStateListener(), true)

    /*
     *  Used by log collector on each adding of the new log record in order to check whether to send logs to server.
     *  Start log upload when there is at least one record in storage.
     */
    val strategy: RecordCountLogUploadStrategy  = new RecordCountLogUploadStrategy(1)
    strategy.setMaxParallelUploads(1)
    kaaClient.setLogUploadStrategy(strategy)

    /*
     * Persist configuration in a local storage to avoid downloading it each
     * time the Kaa client is started.
     */
    kaaClient.setConfigurationStorage(new SimpleConfigurationStorage(desktopKaaPlatformContext, "saved_config.cfg"))

    kaaClient.addConfigurationListener(new ConfigurationListener() {
      override def onConfigurationUpdate(configuration: HeartbeatConfiguration) {
        LOG.info("Received configuration data. New sample period: {}", configuration.getSamplePeriod())
        onChangedConfiguration(TimeUnit.SECONDS.toMillis(configuration.getSamplePeriod().toLong))
      }
    })

    //Start the Kaa client and connect it to the Kaa server.
    kaaClient.start()

    LOG.info("--= Press any key to exit =--")
    try {
      System.in.read()
    } catch {
      case  ioe: IOException => LOG.error("IOException has occurred: {}", ioe.getMessage())
    }
    LOG.info("Stopping...")
    scheduledExecutorService.shutdown()
    kaaClient.stop()
  }
}

class HeartbeatMonitorClient

