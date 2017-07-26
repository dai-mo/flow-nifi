package org.dcs.flow.nifi

import java.nio.file.{Path, Paths}
import java.util.UUID

import org.dcs.api.processor.{CoreProperties, RemoteProcessor}
import org.dcs.api.service.{Connectable, Connection, ConnectionConfig, FlowComponent, FlowInstance}
import org.dcs.commons.SchemaAction
import org.dcs.commons.error.ValidationErrorResponse
import org.dcs.commons.serde.{AvroSchemaStore, JsonPath}
import org.dcs.flow.{FlowBaseUnitSpec, FlowGraph, FlowGraphTraversal, FlowUnitSpec}
import org.dcs.flow.client.FlowApiSpec
import org.dcs.flow.FlowGraph.FlowGraphNode
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.dcs.commons.serde.JsonSerializerImplicits._
import scala.Predef

import scala.concurrent.Future

/**
  * Created by cmathew on 04/08/16.
  */


class FlowGraphSpec extends FlowUnitSpec with NifiFlowGraphBehaviors {
  import FlowApiSpec._

  "Flow Graph Construction" must "generate valid graph" in {

    val processGroupPath: Path = Paths.get(FlowApiSpec.getClass.getResource("process-group.json").toURI)
    val flowInstancePath: Path = Paths.get(FlowApiSpec.getClass.getResource("flow-instance.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(Future.successful(jsonFromFile(flowInstancePath.toFile))).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.flowProcessGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateFlowGraphConstruction(flowClient, FlowInstanceId)
  }

  "Flow Graph Schema Update" must "generate valid graph" in {

    val processGroupPath: Path = Paths.get(FlowApiSpec.getClass.getResource("process-group.json").toURI)
    val flowInstancePath: Path = Paths.get(FlowApiSpec.getClass.getResource("flow-instance.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(Future.successful(jsonFromFile(flowInstancePath.toFile))).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.flowProcessGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateProcessorSchemaUpdate(flowClient, FlowInstanceId)
  }

  "Flow Graph Schema Propagation" must "generate valid graph" in {

    val flowInstancePath: Path = Paths.get(FlowApiSpec.getClass.getResource("flow-instance-not-connected.json").toURI())

    validateProcessorSchemaPropagation(jsonFromFile(flowInstancePath.toFile).toObject[FlowInstance])
  }

  "Flow Graph Schema Update" must "validate processor fields with schema" in {

    val processGroupPath: Path = Paths.get(FlowApiSpec.getClass.getResource("process-group.json").toURI)
    val flowInstancePath: Path = Paths.get(FlowApiSpec.getClass.getResource("flow-instance-invalid-schema.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(Future.successful(jsonFromFile(flowInstancePath.toFile))).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.flowProcessGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateProcessorFieldToSchema(flowClient, FlowInstanceId)
  }
}

trait NifiFlowGraphBehaviors extends FlowBaseUnitSpec {
  this: FlatSpec =>

  def validateFlowGraphConstruction(flowClient: NifiFlowClient, flowInstanceId: String) {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    val graphNodes = FlowGraph.buildFlowGraph(flowInstance)
    assert(graphNodes.count(n => n.parents.isEmpty) == 1)
    assert(graphNodes.count(n => n.children.isEmpty) == 1)
    assert(graphNodes.size == 4)

    def identity(graphNode: FlowGraphNode): FlowGraphNode = graphNode

    val roots = FlowGraph.roots(graphNodes.toList)
    assert(roots.size == 1)
    assert(roots.head.processorInstance.id == "3310c81f-015b-1000-fd45-876024494d80")
    val breadthFirstNodes = FlowGraph.executeBreadthFirst(flowInstance, identity)
    var node = breadthFirstNodes.head
    assert(node.processorInstance.id == roots.head.processorInstance.id)
    node = node.children.head
    assert(node.processorInstance.id == "331261d7-015b-1000-7a65-9a90fda246f2")
    node = node.children.head
    assert(node.processorInstance.id == "3315b3a2-015b-1000-ef05-c53e7faa0351")
    node = node.children.head
    assert(node.processorInstance.id == "331ca006-015b-1000-f9bd-c9f02ecd5766")

    val NodeToFilterProcessorId = "3310c81f-015b-1000-fd45-876024494d80"
    val filteredNode = FlowGraph.filter(graphNodes.toList, (fgn: FlowGraphNode) => fgn.processorInstance.id == NodeToFilterProcessorId).head
    assert(filteredNode.processorInstance.id == NodeToFilterProcessorId)
  }

  def validateProcessorSchemaUpdate(flowClient: NifiFlowClient, flowInstanceId: String): Unit = {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    val graphNodes = FlowGraph.buildFlowGraph(flowInstance)

    val RootNodeProcessorId = "3310c81f-015b-1000-fd45-876024494d80"

    val SciNName = "scientificName"
    val remSciNameAction = SchemaAction(SchemaAction.SCHEMA_REM_ACTION,
      JsonPath.Root + JsonPath.Sep + SciNName)

    val processorsToUpdate =
      FlowGraph.executeBreadthFirstFromNode(flowInstance, FlowGraphTraversal.schemaUpdate(List(remSciNameAction)), RootNodeProcessorId)

    val procs = processorsToUpdate.filter(_.isDefined).map(_.get)
    assert(procs.size == 3)

    val firstProc = procs.head
    assert(firstProc.properties(CoreProperties.ReadSchemaIdKey).isEmpty)
    assert(firstProc.properties(CoreProperties.ReadSchemaKey).isEmpty)
    assert(firstProc.properties(CoreProperties.WriteSchemaIdKey).nonEmpty)
    assert(firstProc.properties(CoreProperties.WriteSchemaKey).nonEmpty)

    val secondProc = procs.tail.head
    assert(secondProc.properties(CoreProperties.ReadSchemaIdKey).nonEmpty)
    assert(secondProc.properties(CoreProperties.ReadSchemaKey).nonEmpty)
    assert(secondProc.properties(CoreProperties.WriteSchemaIdKey).isEmpty)
    assert(secondProc.properties(CoreProperties.WriteSchemaKey).isEmpty)

    val thirdProc = procs.tail.tail.head
    assert(thirdProc.properties(CoreProperties.ReadSchemaIdKey).nonEmpty)
    assert(thirdProc.properties(CoreProperties.ReadSchemaKey).nonEmpty)
    assert(thirdProc.properties(CoreProperties.WriteSchemaIdKey).isEmpty)
    assert(thirdProc.properties(CoreProperties.WriteSchemaKey).isEmpty)

  }

  def validateProcessorSchemaPropagation(flowInstance: FlowInstance): Unit = {



    val RootProcessorId = "7946f60b-015d-1000-c380-c7852fdbc44e"
    val ChildOfRootProcessorId = "799f9ebb-015d-1000-0376-0bfcd28c9d8f"
    val ChildOfChildOfRootProcessorId= "7999fb82-015d-1000-eb97-2c0ede7d20e4"

    val rootProcessor = flowInstance.processors.find(_.id == RootProcessorId).get
    val childOfRootProcessor = flowInstance.processors.find(_.id == ChildOfRootProcessorId).get
    val childOfChildOfRootProcessor = flowInstance.processors.find(_.id == ChildOfChildOfRootProcessorId).get



    val firstConnection = new Connection(UUID.randomUUID().toString,
      "",
      1.toLong,
      ConnectionConfig(flowInstance.id,
        Connectable(RootProcessorId, FlowComponent.ProcessorType, flowInstance.id, Map()),
        Connectable(ChildOfRootProcessorId, FlowComponent.ProcessorType, flowInstance.id, Map()),
        Set("success"),
        Set("success", "failure")),
      "",
      "",
      1.toLong,
      Nil)

    val secondConnection = new Connection(UUID.randomUUID().toString,
      "",
      1.toLong,
      ConnectionConfig(flowInstance.id,
        Connectable(ChildOfRootProcessorId, FlowComponent.ProcessorType, flowInstance.id, Map()),
        Connectable(ChildOfChildOfRootProcessorId, FlowComponent.ProcessorType, flowInstance.id, Map()),
        Set("success"),
        Set("success", "failure")),
      "",
      "",
      1.toLong,
      Nil)
    flowInstance.setConnections(List(firstConnection, secondConnection))


    val TestSchema = AvroSchemaStore.get(rootProcessor.properties(CoreProperties.WriteSchemaIdKey)).get.toString

    rootProcessor.setProperties(rootProcessor.properties + (CoreProperties.WriteSchemaKey -> TestSchema))
    val rootCoreProperties =  CoreProperties(rootProcessor.properties)

      FlowGraph.executeBreadthFirstFromNode(flowInstance,
        FlowGraphTraversal.schemaPropagate(RootProcessorId,CoreProperties(rootProcessor.properties)),
        RootProcessorId)

    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey).nonEmpty)
    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey) ==
      rootCoreProperties.writeSchemaId.get)

    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaKey).nonEmpty)
    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaKey) ==
      rootCoreProperties.writeSchema.get.toString)

    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey).nonEmpty)
    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey) ==
      rootCoreProperties.writeSchemaId.get)

    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaKey).nonEmpty)
    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaKey) ==
      rootCoreProperties.writeSchema.get.toString)

    FlowGraph.executeBreadthFirstFromNode(flowInstance,
      FlowGraphTraversal.schemaUnPropagate(RootProcessorId,CoreProperties(rootProcessor.properties)),
      RootProcessorId)

    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey).isEmpty)
    assert(childOfRootProcessor.properties(CoreProperties.ReadSchemaKey).isEmpty)
    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaIdKey).isEmpty)
    assert(childOfChildOfRootProcessor.properties(CoreProperties.ReadSchemaKey).isEmpty)
    
  }

  def validateProcessorFieldToSchema(flowClient: NifiFlowClient, flowInstanceId: String): Unit = {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    val graphNodes = FlowGraph.buildFlowGraph(flowInstance)

    val RootNodeProcessorId = "3310c81f-015b-1000-fd45-876024494d80"

    val SciNName = "scientificName"
    val remSciNameAction = SchemaAction(SchemaAction.SCHEMA_REM_ACTION,
      JsonPath.Root + JsonPath.Sep + SciNName)

    val pis = FlowGraph.executeBreadthFirstFromNode(flowInstance, FlowGraphTraversal.schemaUpdate(List(remSciNameAction)), RootNodeProcessorId)

    val vinfo = pis
      .filter(pi => pi.isDefined && pi.get.processorType == RemoteProcessor.WorkerProcessorType)
      .flatMap(_.get.validationErrors.validationInfo)

    assert(vinfo.size == 3)
    assert(vinfo.count(_(ValidationErrorResponse.ErrorCode) == "DCS310") == 3)

  }
}