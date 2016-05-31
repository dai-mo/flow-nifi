package org.dcs.flow

import org.dcs.flow.model.Flow

trait FlowClient {
  def instantiate(flowTemplateId:String ):Flow
}