package org.dcs.iot.kaa

import scala.concurrent.ExecutionContext.Implicits._

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


class KaaApiInitSpec {

}

class KaaApiInitISpec extends KaaIoTClientUnitSpec {

  "Initialisation of Kaa IoT Platform with DCS config" must "be valid" taggedAs IT in {
    val kaaClientConfig = KaaClientConfig()
    val credentials = kaaClientConfig.credentials
    assert(credentials.isDefined)
    val applicationConfig = kaaClientConfig.applicationConfig
    assert(applicationConfig.isDefined)

    val kaaIoTClient = KaaIoTClient()
    kaaIoTClient.setupCredentials()
      .flatMap(response => kaaIoTClient.createApplication())
      .futureValue
  }

}