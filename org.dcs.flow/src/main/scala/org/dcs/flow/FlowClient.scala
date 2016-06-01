package org.dcs.flow

import org.dcs.flow.model.FlowInstance

trait FlowClient {
  def instantiate(flowTemplateId:String ):FlowInstance
}