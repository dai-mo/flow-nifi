package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.api.error.RESTException
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiApiConfig, NifiProcessorApi, NifiProcessorClient}
import org.mockito.Mockito._
import org.mockito.{Matchers, Mockito}
import org.scalatest.{FlatSpec, Ignore}
import org.slf4j.{Logger, LoggerFactory}




object ProcessorApiSpec {

  val GFFPName = "GenerateFlowFile"
  val GFFPType = "org.apache.nifi.processors.standard.GenerateFlowFile"
  val GFPId = "932d8069-3a9a-42f3-93ee-53f3ea0cc7bc"

  val ClientToken = "29474d0f-3e21-4136-90fd-ad4e2c613afb"
  val UserId = "root"

  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])
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
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]]
      )
    validateProcessorTypes(processorClient)
  }

  "A Processor" must "have valid state when the state is updated" in {

    val ProcessorInstanceId = "57477ce5-f9a1-4b96-b2e0-8e7aa9c68c62"
    val processorStartPath: Path = Paths.get(this.getClass().getResource("start-processor.json").toURI())
    val processorStopPath: Path = Paths.get(this.getClass().getResource("stop-processor.json").toURI())
    var processorApi = Mockito.spy(new NifiProcessorApi())

    validateInvalidProcessorStateChange(processorApi, UserId, 0.0.toLong)

    doReturn(jsonFromFile(processorStartPath.toFile)).
      when(processorApi).
      putAsJson(
        Matchers.eq(NifiProcessorClient.processorsPath(ProcessorInstanceId)),
        Matchers.any[Form],
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    validateProcessorStart(processorApi, ProcessorInstanceId, 0.0.toLong)

    processorApi = Mockito.spy(new NifiProcessorApi())

    doReturn(jsonFromFile(processorStopPath.toFile)).
      when(processorApi).
      putAsJson(
        Matchers.eq(NifiProcessorClient.processorsPath(ProcessorInstanceId)),
        Matchers.any[Form],
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    validateProcessorStop(processorApi, ProcessorInstanceId, 0.0.toLong)
  }


  // FIXME: Re-check implementations of create and remove

//  "A Processor" must "should be created and removed correctly" in {
//
//    val processorCreationPath: Path = Paths.get(this.getClass().getResource("create-gf-processor.json").toURI())
//    val processorApi = Mockito.spy(new NifiProcessorApi())
//
//    doReturn(jsonFromFile(processorCreationPath.toFile)).
//      when(processorApi).
//      postAsJson(
//        Matchers.eq(NifiProcessorClient.processorsPath(UserId)),
//        Matchers.any[Form],
//        Matchers.eq(Map(
//          "name" -> GFFPName,
//          "type" -> GFFPType,
//          "x" -> "17",
//          "y" -> "100")),
//        Matchers.any[List[(String, String)]],
//        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
//      )
//
//    val processorDeletionPath: Path = Paths.get(this.getClass().getResource("delete-gf-processor.json").toURI())
//    doReturn(jsonFromFile(processorDeletionPath.toFile)).
//      when(processorApi).
//      deleteAsJson(
//        Matchers.eq(NifiProcessorClient.processorsPath(UserId) + "/" + GFPId),
//        Matchers.any[Map[String, String]],
//        Matchers.any[List[(String, String)]]
//      )
//    validateProcessorLifecycle(processorApi)
//  }
}

trait ProcessorApiBehaviors { this: FlatSpec =>
  import ProcessorApiSpec._


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

  def validateProcessorStart(processorApi: NifiProcessorApi, processorInstanceId: String, currentVersion: Long) {
    val processor = processorApi.start(processorInstanceId, currentVersion, UserId)
    assert(processor.getStatus() == NifiProcessorClient.StateRunning)
  }

  def validateProcessorStop(processorApi: NifiProcessorApi, processorInstanceId: String, currentVersion: Long) {
    val processor = processorApi.start(processorInstanceId, currentVersion, UserId)
    assert(processor.getStatus() == NifiProcessorClient.StateStopped)
  }

  def validateInvalidProcessorStateChange(processorApi: NifiProcessorApi, processorInstanceId: String, currentVersion: Long) {
    val thrown = intercept[RESTException] {
      processorApi.changeState(processorInstanceId, currentVersion, "THIS_IS_INVALID",  UserId)
    }
    assert(thrown.errorResponse.httpStatusCode == 409)
  }

}