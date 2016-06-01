package org.dcs.flow.nifi

import javax.ws.rs.core.{Form, MediaType}

import org.apache.nifi.web.api.entity.{FlowSnippetEntity, TemplatesEntity}
import org.dcs.flow.model.{FlowInstance, FlowTemplate}
import org.dcs.flow.FlowClient
import org.dcs.commons.JsonSerializerImplicits._
import scala.collection.JavaConverters._


/**
  * Created by cmathew on 30/05/16.
  */

object NifiFlowClient {
  val TemplateInstancePath = "/controller/process-groups/root/template-instance"
  val TemplatesPath = "/controller/templates"
}

trait NifiFlowClient extends FlowClient with NifiBaseRestClient {
  import NifiFlowClient._

  def templates():List[FlowTemplate] = {
    val templates = getAsJson(TemplatesPath).toObject[TemplatesEntity]
    templates.getTemplates.asScala.map(t => FlowTemplate(t)).toList
  }

  def instantiate(flowTemplateId:String ):FlowInstance = {

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

    FlowInstance(flowSnippet)
  }
}
