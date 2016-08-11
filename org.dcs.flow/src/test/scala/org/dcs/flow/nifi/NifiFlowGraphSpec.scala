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
        Matchers.eq(NifiFlowClient.processGroupsPath(UserId) + "/" + FlowInstanceId),
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
    assert(roots.head.processorInstance.id == "66ec429b-0975-4953-9881-7a57103e1b79")
    val breadthFirstNodes = NifiFlowGraph.executeBreadthFirst(flowInstance, identity)
    var node = breadthFirstNodes.head
    assert(node.processorInstance.id == roots.head.processorInstance.id)
    node = node.children.head
    assert(node.processorInstance.id == "d9bd6c32-5f3d-4a85-a43f-09218492b95a")
    node = node.children.head
    assert(node.processorInstance.id == "84579c8a-f861-4325-9e6d-b108b88e3aa6")
    node = node.children.head
    assert(node.processorInstance.id == "3052edb4-7f2f-4a4f-ae31-4f6f8bc13e52")
    node = node.children.head
    assert(node.processorInstance.id == "6460e6a3-0921-4517-b99a-9c34fdc864ff")
  }
}