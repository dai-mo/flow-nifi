package org.dcs.flow.client

import java.util.UUID

import ch.qos.logback.classic.Level
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service._
import org.dcs.commons.error.{ErrorConstants, ErrorResponse, RESTException}
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.dcs.flow.nifi.{FlowProcessorRequest, NifiFlowApi, NifiProcessorApi, NifiProcessorClient}
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
  val flowApi = new NifiFlowApi()
  val processorApi = new NifiProcessorApi()

  var flowInstance: FlowInstance = _

  var gbifP: ProcessorInstance = _
  var latlongP: ProcessorInstance = _
  var filterP: ProcessorInstance = _
  var csvP: ProcessorInstance = _



  override def beforeAll(): Unit = {
    flowApi.requestFilter(new LoggingFilter)
    flowApi.requestFilter(new DetailedLoggingFilter)

    processorApi.requestFilter(new LoggingFilter)
    processorApi.requestFilter(new DetailedLoggingFilter)

    flowInstance = validateFlowCreation(flowApi)
  }

    override def afterAll(): Unit = {
      flowApi.remove(flowInstance.id, flowInstance.version, clientId)
    }

  "Processor Lifecycle" should "execute sucessfully" taggedAs IT in {

    val gbifPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + StatefulGBIFOccurrenceProcessorService,
      RemoteProcessor.IngestionProcessorType,
      true)

    gbifP = validateProcessorCreation(processorApi,
      gbifPsd,
      flowInstance.id,
      clientId)

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    latlongP = validateProcessorCreation(processorApi,
      latLongPsd,
      flowInstance.id,
      clientId)


    val filterPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + FilterProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    filterP = validateProcessorCreation(processorApi,
      filterPsd,
      flowInstance.id,
      clientId)

    val csvPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + CSVFileOutputProcessorService,
      RemoteProcessor.SinkProcessorType,
      true)

    csvP = validateProcessorCreation(processorApi,
      csvPsd,
      flowInstance.id,
      clientId)

    // Start processors
    latlongP = validateProcessorStart(processorApi,
      latlongP.id,
      latlongP.version,
      clientId)

    filterP = validateProcessorStart(processorApi,
      filterP.id,
      filterP.version,
      clientId)

    latlongP = validateProcessorStop(processorApi,
      latlongP.id,
      latlongP.version,
      clientId)

    // Stop processors
    filterP = validateProcessorStop(processorApi,
      filterP.id,
      filterP.version,
      clientId)

    // Remove processors
    validateProcessorRemoval(processorApi, gbifP.id, gbifP.version, clientId)
    validateProcessorRemoval(processorApi, latlongP.id, latlongP.version, clientId)
    validateProcessorRemoval(processorApi, filterP.id, filterP.version, clientId)
    validateProcessorRemoval(processorApi, csvP.id, csvP.version, clientId)
  }


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
                                clientId: String): ProcessorInstance = {
    val processorInstance = processorApi.create(psd, pgId, clientId).futureValue(timeout(5))
    assert(processorInstance.name == psd.processorServiceClassName.split("\\.").last)
    assert(processorInstance.`type` == FlowProcessorRequest.clientProcessorType(psd))
    assert(processorInstance.processorType == psd.processorType)
    processorInstance
  }

  def validateProcessorStart(processorApi: ProcessorApiService,
                            processorId: String,
                            version: Long,
                            clientId: String): ProcessorInstance = {
    val processorInstance = processorApi.start(processorId, version, clientId).futureValue(timeout(5))
    assert(processorInstance.status == NifiProcessorClient.StateRunning)
    processorInstance
  }

  def validateProcessorStop(processorApi: ProcessorApiService,
                             processorId: String,
                             version: Long,
                             clientId: String): ProcessorInstance = {
    val processorInstance = processorApi.stop(processorId, version, clientId).futureValue(timeout(5))
    assert(processorInstance.status == NifiProcessorClient.StateStopped)
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


