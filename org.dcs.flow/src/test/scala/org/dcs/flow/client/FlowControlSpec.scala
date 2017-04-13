package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{CoreProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.{ErrorConstants, RESTException}
import org.dcs.flow.nifi.{FlowProcessorRequest, NifiFlowApi, NifiProcessorApi, NifiProcessorClient}
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.glassfish.jersey.filter.LoggingFilter


/**
  * Created by cmathew on 11.04.17.
  */

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

    var gbifP = validateProcessorCreation(processorApi,
      gbifPsd,
      flowInstance.id,
      clientId)

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    var latlongP = validateProcessorCreation(processorApi,
      latLongPsd,
      flowInstance.id,
      clientId)


    val filterPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + FilterProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    var filterP = validateProcessorCreation(processorApi,
      filterPsd,
      flowInstance.id,
      clientId)

    val csvPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + CSVFileOutputProcessorService,
      RemoteProcessor.SinkProcessorType,
      true)

    var csvP = validateProcessorCreation(processorApi,
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

  "Processor Update" should "execute sucessfully" taggedAs IT in {

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    val latlongP = validateProcessorCreation(processorApi,
      latLongPsd,
      flowInstance.id,
      clientId)

    val fieldsToMap = "{latitude:$.decimalLatitude, longitude:$.decimalLongitude}"
    latlongP.setProperties(latlongP.properties - CoreProperties.FieldsToMapKey + (CoreProperties.FieldsToMapKey -> fieldsToMap))
    validateProcessorPropertyUpdate(processorApi, latlongP, clientId, CoreProperties.FieldsToMapKey, fieldsToMap)
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

  def validateProcessorPropertyUpdate(processorApi: ProcessorApiService,
                                      processorInstance: ProcessorInstance,
                                      clientId: String,
                                      property: String,
                                      value: String): Unit = {
    val updatedProcessorInstance = processorApi.update(processorInstance, clientId).futureValue(timeout(5))
    assert(updatedProcessorInstance.properties(property) == value)
  }
}


