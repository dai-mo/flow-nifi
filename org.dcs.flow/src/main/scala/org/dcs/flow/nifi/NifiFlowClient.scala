package org.dcs.flow.nifi

import javax.ws.rs.core.{Form, MediaType}

import org.apache.nifi.web.api.entity.FlowSnippetEntity
import org.dcs.flow.model.Flow
import org.dcs.flow.FlowClient
import org.dcs.commons.JsonSerializerImplicits._


/**
  * Created by cmathew on 30/05/16.
  */

object NifiFlowClient {
  val TemplateInstancePath = "/controller/process-groups/root/template-instance"
}
trait NifiFlowClient extends FlowClient with NifiBaseRestClient {
  import NifiFlowClient._

  def instantiate(flowTemplateId:String ):Flow = {

    val queryParams = Map(
      "templateId" -> flowTemplateId,
      "originX" -> "17",
      "originY" -> "100"
    )

    val flowSnippet = postAsJson(TemplateInstancePath,
      new Form(),
      queryParams,
      Map(),
      MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[FlowSnippetEntity]

    Flow(flowSnippet)
  }
}
