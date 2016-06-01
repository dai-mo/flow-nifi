import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.commons.JsonUtil
import org.dcs.flow.client.FlowApi
import org.dcs.flow.nifi.{NifiApiConfig, NifiFlowClient}
import org.glassfish.jersey.filter.LoggingFilter

object NifiFlowApi extends FlowApi
  with NifiFlowClient
  with NifiApiConfig


NifiFlowApi.requestFilter(new LoggingFilter())

val templates = NifiFlowApi.templates
JsonUtil.prettyPrint(templates.toJson)

val templateId = "d73b5a44-5968-47d5-9a9a-aea5664c5835"
val flowInstance = NifiFlowApi.instantiate(templateId)
JsonUtil.prettyPrint(flowInstance.toJson)

val processor = flowInstance.processors.head
val processorInstance = NifiProcessorApi.start(processor.id)


