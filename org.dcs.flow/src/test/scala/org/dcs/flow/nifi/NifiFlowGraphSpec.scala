package org.dcs.flow.nifi

import java.nio.file.{Path, Paths}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.client.FlowApiSpec
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
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(49.0.toLong).
      when(flowClient).
      currentVersion()

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
  }
}