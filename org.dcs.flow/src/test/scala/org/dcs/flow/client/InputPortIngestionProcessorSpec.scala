package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service.{ProcessorApiService, ProcessorServiceDefinition}
import org.dcs.flow.nifi.{NifiFlowApi, NifiProcessorApi}
import org.dcs.flow.{AsyncFlowUnitSpec, IT}
import org.scalatest.Assertion

import scala.concurrent.Future

object InputPortIngestionSpec {
  val ClientId: String = UUID.randomUUID().toString
  val FlowInstancename = "InputPortIngestionProcessorTest"
  val ServiceClassPrefix = "org.dcs.core.service."
  val KaaIngestionProcessorService = "KaaIngestionProcessorService"

  val kaaPsd = ProcessorServiceDefinition(
    ServiceClassPrefix + KaaIngestionProcessorService,
    RemoteProcessor.InputPortIngestionType,
    false)

  val flowApi = new NifiFlowApi
  val processorApi = new NifiProcessorApi
}

class InputPortIngestionProcessorSpec extends InputPortIngestionBehaviour {

}

class InputPortIngestionProcessorISpec extends InputPortIngestionBehaviour {
  import InputPortIngestionSpec._

  "Creation / Deletion of Input Ingestion Processor" should "be valid" taggedAs IT in {
    val flowInstance = flowApi.create(FlowInstancename, ClientId).futureValue
    validateCreateInputPortIngestionProcessor(processorApi, kaaPsd, flowInstance.id)
  }
}

trait InputPortIngestionBehaviour extends AsyncFlowUnitSpec {
  import InputPortIngestionSpec._

  def validateCreateInputPortIngestionProcessor(processorApi: ProcessorApiService,
                                                psd: ProcessorServiceDefinition,
                                                flowInstanceId: String): Future[Assertion] = {
    processorApi.create(psd, flowInstanceId, ClientId)
      .map { p =>
        assert(p.processorType == RemoteProcessor.InputPortIngestionType)
      }
  }

}