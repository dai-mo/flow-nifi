package org.dcs.flow.nifi

import java.nio.file.{Path, Paths}

import org.dcs.flow.{FlowBaseUnitSpec, FlowUnitSpec}
import org.dcs.flow.client.FlowApiSpec
import org.dcs.flow.nifi.NifiFlowGraph.FlowGraphNode
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec

import scala.concurrent.Future

/**
  * Created by cmathew on 04/08/16.
  */


class NifiFlowGraphSpec extends FlowUnitSpec with NifiFlowGraphBehaviors {
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
}

trait NifiFlowGraphBehaviors extends FlowBaseUnitSpec {
  this: FlatSpec =>

  import FlowApiSpec._

  def validateFlowGraphConstruction(flowClient: NifiFlowClient, flowInstanceId: String) {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    val graphNodes = NifiFlowGraph.buildFlowGraph(flowInstance)
    assert(graphNodes.count(n => n.parents.isEmpty) == 1)
    assert(graphNodes.count(n => n.children.isEmpty) == 1)
    assert(graphNodes.size == 4)

    def identity(graphNode: FlowGraphNode): FlowGraphNode = graphNode

    val roots = NifiFlowGraph.roots(graphNodes.toList)
    assert(roots.size == 1)
    assert(roots.head.processorInstance.id == "3310c81f-015b-1000-fd45-876024494d80")
    val breadthFirstNodes = NifiFlowGraph.executeBreadthFirst(flowInstance, identity)
    var node = breadthFirstNodes.head
    assert(node.processorInstance.id == roots.head.processorInstance.id)
    node = node.children.head
    assert(node.processorInstance.id == "331261d7-015b-1000-7a65-9a90fda246f2")
    node = node.children.head
    assert(node.processorInstance.id == "3315b3a2-015b-1000-ef05-c53e7faa0351")
    node = node.children.head
    assert(node.processorInstance.id == "331ca006-015b-1000-f9bd-c9f02ecd5766")
  }
}