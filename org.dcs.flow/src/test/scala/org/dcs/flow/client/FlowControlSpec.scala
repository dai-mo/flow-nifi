/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{CoreProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.HttpException
import org.dcs.flow.nifi.{FlowInstance => _, ProcessorInstanceAdapter => _, _}
import org.dcs.flow.{FlowUnitSpec, IT}


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

  val ClientId: String = UUID.randomUUID().toString
  val flowApi = new NifiFlowApi
  val processorApi = new NifiProcessorApi
  val connectionApi = new NifiConnectionApi
  var flowInstance: FlowInstance = _


  override def beforeAll(): Unit = {
    flowInstance = validateFlowCreation(flowApi, ClientId)
  }

  override def afterAll(): Unit = {
    flowApi.remove(flowInstance.id, flowInstance.version, ClientId)
  }

  "Processor Lifecycle" should "execute sucessfully" taggedAs IT in {

    val gbifPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + StatefulGBIFOccurrenceProcessorService,
      RemoteProcessor.IngestionProcessorType,
      true)

    var gbifP = validateProcessorCreation(processorApi,
      gbifPsd,
      flowInstance.id,
      ClientId)

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    var latlongP = validateProcessorCreation(processorApi,
      latLongPsd,
      flowInstance.id,
      ClientId)


    val filterPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + FilterProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    var filterP = validateProcessorCreation(processorApi,
      filterPsd,
      flowInstance.id,
      ClientId)

    val csvPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + CSVFileOutputProcessorService,
      RemoteProcessor.SinkProcessorType,
      true)

    var csvP = validateProcessorCreation(processorApi,
      csvPsd,
      flowInstance.id,
      ClientId)

    // Connect GBIF processor with LatLong processor
    val gbifLatLongConnection = validateConnectionCreationUpdate(connectionApi,
      flowInstance.id,
      Connectable(gbifP.id, FlowComponent.ProcessorType, flowInstance.id),
      Connectable(latlongP.id, FlowComponent.ProcessorType, flowInstance.id),
      Set("success"),
      Set("success", "failure"),
      "gbif-latlong",
      ClientId)

    // Connect latLong processor with Filter processor
    val latLongFilterConnection = validateConnectionCreationUpdate(connectionApi,
      flowInstance.id,
      Connectable(latlongP.id, FlowComponent.ProcessorType, flowInstance.id),
      Connectable(filterP.id, FlowComponent.ProcessorType, flowInstance.id),
      Set("valid"),
      Set("valid", "invalid", "failure"),
      "latlong-filter",
      ClientId)

    // Start processors
    latlongP = validateProcessorStart(processorApi,
      latlongP.id,
      latlongP.version,
      ClientId)

    filterP = validateProcessorStart(processorApi,
      filterP.id,
      filterP.version,
      ClientId)

    latlongP = validateProcessorStop(processorApi,
      latlongP.id,
      latlongP.version,
      ClientId)

    // Stop processors
    filterP = validateProcessorStop(processorApi,
      filterP.id,
      filterP.version,
      ClientId)

    // Remove Connections
    connectionApi.remove(gbifLatLongConnection.id, gbifLatLongConnection.version, ClientId)
    connectionApi.remove(latLongFilterConnection.id, latLongFilterConnection.version, ClientId)

    // Remove processors
    validateProcessorRemoval(processorApi, gbifP.id, gbifP.version, ClientId)
    validateProcessorRemoval(processorApi, latlongP.id, latlongP.version, ClientId)
    validateProcessorRemoval(processorApi, filterP.id, filterP.version, ClientId)
    validateProcessorRemoval(processorApi, csvP.id, csvP.version, ClientId)
  }

  "Processor Update" should "execute sucessfully" taggedAs IT in {

    val latLongPsd = ProcessorServiceDefinition(
      ServiceClassPrefix + LatLongValidationProcessorService,
      RemoteProcessor.WorkerProcessorType,
      false)

    val latlongP = validateProcessorCreation(processorApi,
      latLongPsd,
      flowInstance.id,
      ClientId)

    val fieldsToMap = "{latitude:$.decimalLatitude, longitude:$.decimalLongitude}"
    latlongP.setProperties(latlongP.properties - CoreProperties.FieldsToMapKey + (CoreProperties.FieldsToMapKey -> fieldsToMap))
    validateProcessorPropertyUpdate(processorApi, latlongP, ClientId, CoreProperties.FieldsToMapKey, fieldsToMap)
  }
}

trait FlowCreationBehaviours extends FlowUnitSpec {

  import FlowControlSpec._

  def validateFlowCreation(flowApi: FlowApiService, clientId: String): FlowInstance = {
    val flowInstance = flowApi.create(FlowInstanceName, clientId).futureValue(timeout(5))
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
            ex shouldBe a [HttpException]
            assert(ex.asInstanceOf[HttpException].errorResponse.httpStatusCode == 404)
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

  def validateConnectionCreationUpdate(connectionApi: ConnectionApiService,
                                       flowInstanceId: String,
                                       sourceConnectable: Connectable,
                                       destinationConnectable: Connectable,
                                       selectedRelationships: Set[String],
                                       availableRelationships: Set[String],
                                       name: String,
                                       clientId: String): Connection = {
    val connectionCreate = new ConnectionConfig(flowInstanceId,
      sourceConnectable,
      destinationConnectable,
      selectedRelationships,
      availableRelationships)

    var connection = connectionApi.create(connectionCreate, clientId).futureValue(timeout(5))
    connection.setName(name)
    connection = connectionApi.update(connection, clientId).futureValue(timeout(5))
    assert(connection.name == name)
    connection
  }
}


