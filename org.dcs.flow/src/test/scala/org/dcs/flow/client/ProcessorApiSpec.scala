package org.dcs.flow.client

import java.io.File
import java.nio.file.{Path, Paths}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiApiConfig, NifiProcessorClient}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}




object ProcessorApiSpec {
  class NifiProcessorApi extends ProcessorApi
    with NifiProcessorClient
    with NifiApiConfig
}

class ProcessorApiSpec extends RestBaseUnitSpec with ProcessorApiBehaviors {

  import ProcessorApiSpec._


  "Processor Types" must " be valid " in {
    val typesPath: Path = Paths.get(this.getClass().getResource("types.json").toURI())
    val processorClient = Mockito.spy(new NifiProcessorApi())
    doReturn(jsonFromFile(typesPath.toFile)).
      when(processorClient).
      getAsJson(NifiProcessorClient.TypesPath, Map(), Map())
    validateProcessorTypes(processorClient)
  }
}

trait ProcessorApiBehaviors { this: FlatSpec =>
  import ProcessorApiSpec._

  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])

  def validateProcessorTypes(processorApi: NifiProcessorApi) {
    val types = processorApi.types()
    assert(types.size == 135)
  }

  def validateProcessorStart(processorApi: NifiProcessorApi, processorId: String): Unit = {
    processorApi.start(processorId)
  }
}