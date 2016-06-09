import java.util.UUID

import org.dcs.flow.nifi.{NifiApiConfig, NifiFlowClient}
import org.glassfish.jersey.filter.LoggingFilter

object NifiFlowApi extends NifiFlowClient
  with NifiApiConfig

val clientId = UUID.randomUUID.toString
NifiFlowApi.requestFilter(new LoggingFilter())

//val templates = NifiFlowApi.templates
//JsonUtil.prettyPrint(templates.toJson)
//
val templateId = "d73b5a44-5968-47d5-9a9a-aea5664c5835"
//val flowInstance = NifiFlowApi.instantiate(templateId, clientId)
//flowInstance.toJsonP


val response = NifiFlowApi.remove("root", clientId)

//val flowInstance = NifiFlowApi.instance("root")
//JsonUtil.prettyPrint(flowInstance.toJson)

//NifiFlowApi.delete("root")

//val flowInstance = NifiFlowApi.instance("root", clientId)
//flowInstance.toJsonP




