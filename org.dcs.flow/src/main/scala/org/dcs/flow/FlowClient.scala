package org.dcs.flow

import org.dcs.flow.model.FlowInstance

trait FlowClient {
  def instantiate(flowTemplateId:String, clientId: String):FlowInstance
  def instance(flowInstanceId: String, clientId: String): FlowInstance
  def remove(flowInstanceId: String, clientId: String): Boolean
}