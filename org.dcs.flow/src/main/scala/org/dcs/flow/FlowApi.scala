package org.dcs.flow

import org.dcs.flow.model.Flow

trait FlowApi {
  def instantiate(flowTemplateId:String ):Flow
}