package org.dcs.flow.nifi

import java.nio.file.{Path, Paths}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.client.FlowApiSpec
import org.dcs.flow.nifi.NifiFlowGraph.FlowGraphNode
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec

/**
  * Created by cmathew on 04/08/16.
  */


class NifiFlowGraphSpec extends RestBaseUnitSpec with NifiFlowGraphBehaviors {
  import FlowApiSpec._

  "Flow Graph Construction" must "generate valid graph" in {

    val flowInstancePath: Path = Paths.get(FlowApiSpec.getClass.getResource("flow-instance.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(jsonFromFile(flowInstancePath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.flowProcessGroupsPath(FlowInstanceId)),
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]]
      )

//    doReturn(49.0.toLong).
//      when(flowClient).
//      currentVersion()

    validateFlowGraphConstruction(flowClient, FlowInstanceId)
  }
}

trait NifiFlowGraphBehaviors {
  this: FlatSpec =>

  import FlowApiSpec._

  def validateFlowGraphConstruction(flowClient: NifiFlowClient, flowInstanceId: String) {
    val flowInstance = flowClient.instance(flowInstanceId, UserId, ClientToken)
    val graphNodes = NifiFlowGraph.buildFlowGraph(flowInstance)
    assert(graphNodes.count(n => n.parents.isEmpty) == 1)
    assert(graphNodes.count(n => n.children.isEmpty) == 1)
    assert(graphNodes.size == 5)

    def identity(graphNode: FlowGraphNode): FlowGraphNode = graphNode

    val roots = NifiFlowGraph.roots(graphNodes.toList)
    assert(roots.size == 1)
    assert(roots.head.processorInstance.id == "3fde726d-5cc1-4bb6-6428-3e26c236b45d")
    val breadthFirstNodes = NifiFlowGraph.executeBreadthFirst(flowInstance, identity)
    var node = breadthFirstNodes.head
    assert(node.processorInstance.id == roots.head.processorInstance.id)
    node = node.children.head
    assert(node.processorInstance.id == "daf07177-3fe2-42a8-2570-ee13e230d491")
    node = node.children.head
    assert(node.processorInstance.id == "13613f8e-d402-4018-9e4d-c508c7948cfa")
    node = node.children.head
    assert(node.processorInstance.id == "55c734e3-9e8c-4d8e-ba40-3f2d1959fe33")
    node = node.children.head
    assert(node.processorInstance.id == "c8d56491-4d2e-4751-be71-672fa3b35516")
  }
}