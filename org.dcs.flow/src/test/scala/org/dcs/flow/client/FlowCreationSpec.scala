package org.dcs.flow.client

import java.util.UUID

import ch.qos.logback.classic.Level
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service._
import org.dcs.flow.{FlowUnitSpec, IT}
import org.dcs.flow.nifi.{FlowProcessorRequest, NifiFlowApi, NifiProcessorApi}
import org.glassfish.jersey.filter.LoggingFilter
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by cmathew on 11.04.17.
  */

import javax.ws.rs.client.ClientRequestContext
import javax.ws.rs.client.ClientRequestFilter
import java.io.IOException

object DetailedLoggingFilter {
  private val LOG = LoggerFactory.getLogger(classOf[DetailedLoggingFilter].getName)
}

class DetailedLoggingFilter extends ClientRequestFilter {

  @throws[IOException]
  override def filter(requestContext: ClientRequestContext): Unit = {
    DetailedLoggingFilter.LOG.info(requestContext.getEntity.toString)
  }
}

object FlowCreationSpec {
  val FlowInstanceName = "test-flow"
  val ServiceClassPrefix = "org.dcs.core.service."
  val StatefulGBIFOccurrenceProcessorService = "StatefulGBIFOccurrenceProcessorService"
}

class FlowCreationSpec extends FlowCreationBehaviours {

}

class FlowCreationISpec extends FlowCreationBehaviours {
  import FlowCreationSpec._

  "Flow Creation Lifecycle" should "be valid" taggedAs IT in {
    val flowApiService = new NifiFlowApi()
    flowApiService.requestFilter(new LoggingFilter)
    flowApiService.requestFilter(new DetailedLoggingFilter)

    val processorApiService = new NifiProcessorApi()
    processorApiService.requestFilter(new LoggingFilter)
    processorApiService.requestFilter(new DetailedLoggingFilter)

    val flowInstance = validateFlowCreation(flowApiService)

    val ipsd = new ProcessorServiceDefinition(
      ServiceClassPrefix + StatefulGBIFOccurrenceProcessorService,
    RemoteProcessor.IngestionProcessorType,
    true)

    val processorInstance = validateProcessorCreation(processorApiService,
      ipsd,
      flowInstance.id)
  }
}

trait FlowCreationBehaviours extends FlowUnitSpec {

  import FlowCreationSpec._

  def validateFlowCreation(flowApi: FlowApiService): FlowInstance = {
    val flowInstance = flowApi.create(FlowInstanceName).futureValue
    assert(flowInstance.name == FlowInstanceName)
    assert(UUID.fromString(flowInstance.nameId) != null)
    assert(UUID.fromString(flowInstance.id) != null)
    flowInstance
  }

  def validateProcessorCreation(processorApi: ProcessorApiService,
                                psd: ProcessorServiceDefinition,
                                pgId: String): ProcessorInstance = {
    val processorInstance = processorApi.create(psd, pgId).futureValue
    assert(processorInstance.name == psd.processorServiceClassName.split("\\.").last)
    assert(processorInstance.`type` == FlowProcessorRequest.clientProcessorType(psd))
    assert(processorInstance.processorType == psd.processorType)
    processorInstance
  }
}


