package org.dcs.flow.nifi

import java.nio.file.{Path, Paths}

import org.dcs.api.processor.CoreProperties
import org.dcs.commons.SchemaAction
import org.dcs.commons.serde.JsonPath
import org.dcs.flow.{FlowBaseUnitSpec, FlowGraph, FlowGraphTraversal, FlowUnitSpec}
import org.dcs.flow.client.FlowApiSpec
import org.dcs.flow.FlowGraph.FlowGraphNode
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec

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

  def validateProcessorFieldToSchema(flowClient: NifiFlowClient, flowInstanceId: String): Unit = {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    val graphNodes = FlowGraph.buildFlowGraph(flowInstance)

    val RootNodeProcessorId = "3310c81f-015b-1000-fd45-876024494d80"

    val SciNName = "scientificName"
    val remSciNameAction = SchemaAction(SchemaAction.SCHEMA_REM_ACTION,
      JsonPath.Root + JsonPath.Sep + SciNName)

    assertThrows[IllegalStateException] {
      FlowGraph.executeBreadthFirstFromNode(flowInstance, FlowGraphTraversal.schemaUpdate(List(remSciNameAction)), RootNodeProcessorId)
    }



  }
}