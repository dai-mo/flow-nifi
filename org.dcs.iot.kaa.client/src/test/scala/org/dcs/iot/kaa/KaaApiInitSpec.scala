package org.dcs.iot.kaa

import java.util.UUID

import org.apache.avro.Schema

import scala.concurrent.ExecutionContext.Implicits._
import org.dcs.commons.serde.JsonSerializerImplicits._

/**
  * Tests the initialisation of the Kaa IoT Platform with DCS configuration.
  *
  * NOTE: TO run these tests the VM property,
  * -DkaaConfigDir=/path/to/DCS Kaa Config Directory>
  *
  * A sample directory is available at src/test/resources/kaa-config
  *
  * @author cmathew
  */

object KaaApiInitSpec {

  val HeartbeatMonitorApplicationName = "Heartbeat Monitor"
}
class KaaApiInitSpec {

}

class KaaApiInitISpec extends KaaIoTClientUnitSpec {
  import KaaApiInitSpec._

  "Initialisation of Kaa IoT Platform with DCS config" must "be valid" taggedAs IT in {
    val kaaClientConfig = KaaClientConfig()

    val applicationConfig = kaaClientConfig.applicationsConfig
    assert(applicationConfig.isDefined)

    val kaaIoTInitialiser = KaaIoTInitialiser()
    kaaIoTInitialiser.setupCredentials()
      .flatMap(response => kaaIoTInitialiser.createApplications())
      .futureValue
  }

  "Retrieval of Kaa IoT Platform Applications" must "be valid" taggedAs IT in {
    val kaaClientConfig = KaaClientConfig()

    val applicationConfig = kaaClientConfig.applicationsConfig
    assert(applicationConfig.isDefined)

    val kaaIoTClient = KaaIoTClient()
    val applications = kaaIoTClient.applications().futureValue
    assert(applications.size == 2)
  }

  "Creation / Update / Removal of Log Appender" should "be valid" taggedAs IT in {
    val kaaClientConfig = KaaClientConfig()

    val kaaIoTClient = KaaIoTClient()
    val applications = kaaIoTClient.applications().futureValue

    val nifiLogAppender = applications.find(app => app.name == HeartbeatMonitorApplicationName)
        .map { app =>
          kaaIoTClient.application(app.applicationToken)
            .flatMap(app => kaaIoTClient.createLogAppender(app,
              "Nifi S2S Appender",
              "org.dcs.iot.kaa.NifiS2SAppender",
              "Nifi S2S Appender",
              NifiS2SConfig().toJson))
            .futureValue
        }

    assert(nifiLogAppender.isDefined)

    val rootInputPortName = UUID.randomUUID().toString

    val updatedNifiLogAppender =
      kaaIoTClient.updateLogAppenderSettings(nifiLogAppender.get,
        NifiS2SConfig(rootInputPortName).toJson).futureValue


    assert(updatedNifiLogAppender.jsonConfiguration.toObject[NifiS2SConfig].inputPortName == rootInputPortName)

    val retrievedNifiLogAppender = kaaIoTClient.logAppenderWithRootInputPortId(updatedNifiLogAppender.applicationToken, rootInputPortName).futureValue.get

    assert(updatedNifiLogAppender.id == retrievedNifiLogAppender.id)

    val logSchemaWithMaxVersion = kaaIoTClient.logSchemaWithMaxVersion(updatedNifiLogAppender.applicationToken).futureValue

    assert(logSchemaWithMaxVersion.version == 2)

    val applicationLogSchema = kaaIoTClient.maxApplicationLogSchema(updatedNifiLogAppender.applicationToken).futureValue

    val parsedSchema = new Schema.Parser().parse(applicationLogSchema)

    assert(parsedSchema.getNamespace == "org.dcs.iot.kaa.schema.log")
    assert(parsedSchema.getFields.size == 1)
    assert(parsedSchema.getField("heartbeat").name() == "heartbeat")

    assert(kaaIoTClient.removeLogAppenderWithRootInputPortId(updatedNifiLogAppender.applicationToken, rootInputPortName).futureValue)

  }

}