package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import java.util.UUID
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiApiConfig, NifiFlowClient, NifiProcessorClient}
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}




object ProcessorApiSpec {

  val GFFPName = "GenerateFlowFile"
  val GFFPType = "org.apache.nifi.processors.standard.GenerateFlowFile"
  val GFPId = "932d8069-3a9a-42f3-93ee-53f3ea0cc7bc"
  val ClientToken = UUID.randomUUID.toString

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
      getAsJson(
        Matchers.eq(NifiProcessorClient.TypesPath),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )
    validateProcessorTypes(processorClient)
  }

  "Processor Lifecycle" must " be valid " in {

    val processorCreationPath: Path = Paths.get(this.getClass().getResource("create-gf-processor.json").toURI())
    val processorApi = Mockito.spy(new NifiProcessorApi())

    doReturn(jsonFromFile(processorCreationPath.toFile)).
      when(processorApi).
      postAsJson(
        Matchers.eq(NifiProcessorClient.ProcessorsPath),
        Matchers.any[Form],
        Matchers.eq(List(("name",GFFPName), ("type", GFFPType), ("x", "17"), ("y","100"))),
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    val processorDeletionPath: Path = Paths.get(this.getClass().getResource("delete-gf-processor.json").toURI())
    doReturn(jsonFromFile(processorDeletionPath.toFile)).
      when(processorApi).
      deleteAsJson(
        Matchers.eq(NifiProcessorClient.ProcessorsPath + "/" + GFPId),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )
    validateProcessorLifecycle(processorApi)
  }
}

trait ProcessorApiBehaviors { this: FlatSpec =>
  import ProcessorApiSpec._

  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])

  def validateProcessorTypes(processorApi: NifiProcessorApi) {
    val types = processorApi.types(ClientToken)
    assert(types.size == 135)
  }

  def validateProcessorLifecycle(processorApi: NifiProcessorApi) {

    //    val p = processorApi.create(GFFPName, GFFPType, ClientToken)
    //    assert(p.status == "STOPPED")
    //
    //    assert(processorApi.remove(p.id, ClientToken))
  }

}