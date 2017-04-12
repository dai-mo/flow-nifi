package org.dcs.flow.client

import java.util.UUID

import ch.qos.logback.classic.Level
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service._
import org.dcs.commons.error.{ErrorConstants, ErrorResponse, RESTException}
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.dcs.flow.nifi.{FlowProcessorRequest, NifiFlowApi, NifiProcessorApi}
import org.glassfish.jersey.filter.LoggingFilter
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by cmathew on 11.04.17.
  */

import javax.ws.rs.client.ClientRequestContext
import javax.ws.rs.client.ClientRequestFilter
import java.io.IOException



object FlowControlSpec {
  val FlowInstanceName = "test-flow"
  val ServiceClassPrefix = "org.dcs.core.service."
  val StatefulGBIFOccurrenceProcessorService = "StatefulGBIFOccurrenceProcessorService"
  val LatLongValidationProcessorService = "LatLongValidationProcessorService"
  val FilterProcessorService = "FilterProcessorService"
  val CSVFileOutputProcessorService = "CSVFileOutputProcessorService"
}

class FlowControlSpec extends FlowCreationBehaviours {

}

class FlowControlISpec extends FlowCreationBehaviours {
  import FlowControlSpec._

  val clientId = UUID.randomUUID().toString
  val flowApiService = new NifiFlowApi()
  val processorApiService = new NifiProcessorApi()

  var flowInstance: FlowInstance = _

  var gbifP: ProcessorInstance = _
  var latlongP: ProcessorInstance = _
  var filterP: ProcessorInstance = _
  var csvP: ProcessorInstance = _

  override def beforeAll(): Unit = {
    flowApiService.requestFilter(new LoggingFilter)
    flowApiService.requestFilter(new DetailedLoggingFilter)

    processorApiService.requestFilter(new LoggingFilter)
    processorApiService.requestFilter(new DetailedLoggingFilter)

    flowInstance = validateFlowCreation(flowApiService)
  }
  //
  //  override def afterAll(): Unit = {
  //
  //  }

  "Processor Creation" should "execute sucessfully" taggedAs IT in {

    val flowInstance = validateFlowCreation(flowApiService)

    val gbifPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + StatefulGBIFOccurrenceProcessorService,
      RemoteProcessor.IngestionProcessorType,
      true)

    gbifP = validateProcessorCreation(processorApiService,
      gbifPsd,
      flowInstance.id,
      clientId)

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    latlongP = validateProcessorCreation(processorApiService,
      latLongPsd,
      flowInstance.id,
      clientId)


    val filterPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + FilterProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    filterP = validateProcessorCreation(processorApiService,
      filterPsd,
      flowInstance.id,
      clientId)

    val csvPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + CSVFileOutputProcessorService,
      RemoteProcessor.SinkProcessorType,
      true)

    csvP = validateProcessorCreation(processorApiService,
      csvPsd,
      flowInstance.id,
      clientId)
  }

//  "Processor Removal" should "execute sucessfully" taggedAs IT in {
//    validateProcessorRemoval(processorApiService, gbifP.id, gbifP.version, clientId)
//    validateProcessorRemoval(processorApiService, latlongP.id, latlongP.version, clientId)
//    validateProcessorRemoval(processorApiService, filterP.id, filterP.version, clientId)
//    validateProcessorRemoval(processorApiService, csvP.id, csvP.version, clientId)
//  }
}

trait FlowCreationBehaviours extends FlowUnitSpec {

  import FlowControlSpec._

  def validateFlowCreation(flowApi: FlowApiService): FlowInstance = {
    val flowInstance = flowApi.create(FlowInstanceName).futureValue(timeout(5))
    assert(flowInstance.name == FlowInstanceName)
    assert(UUID.fromString(flowInstance.nameId) != null)
    assert(UUID.fromString(flowInstance.id) != null)
    flowInstance
  }

  def validateProcessorCreation(processorApi: ProcessorApiService,
                                psd: ProcessorServiceDefinition,
                                pgId: String,
                                clientid: String): ProcessorInstance = {
    val processorInstance = processorApi.create(psd, pgId, clientid).futureValue(timeout(5))
    assert(processorInstance.name == psd.processorServiceClassName.split("\\.").last)
    assert(processorInstance.`type` == FlowProcessorRequest.clientProcessorType(psd))
    assert(processorInstance.processorType == psd.processorType)
    processorInstance
  }

  def validateProcessorRemoval(processorApi: ProcessorApiService,
                               processorId: String,
                               version: Long,
                               clientId: String): Unit = {

    whenReady(processorApi.remove(processorId, version, clientId)) {
      deleted => {
        assert(deleted)
        whenReady(processorApi.instance(processorId).failed) {
          ex => {
            ex shouldBe a [RESTException]
            assert(ex.asInstanceOf[RESTException].errorResponse == ErrorConstants.DCS304)
          }
        }
      }
    }
  }
}


