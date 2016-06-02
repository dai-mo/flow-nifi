import java.util.UUID

import org.dcs.api.service.RESTException
import org.dcs.commons.JsonUtil
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.client.ProcessorApi
import org.dcs.flow.nifi.{NifiApiConfig, NifiProcessorClient}
import org.glassfish.jersey.filter.LoggingFilter

object NifiProcessorApi extends ProcessorApi
  with NifiProcessorClient
  with NifiApiConfig

NifiProcessorApi.requestFilter(new LoggingFilter())

val ClientToken = UUID.randomUUID.toString
val GFFPName = "GenerateFlowFile"
val GFFPType = "org.apache.nifi.processors.standard.GenerateFlowFile"

val LAPName = "LogAttribute"
val LAPType = "org.apache.nifi.processors.standard.LogAttribute"
try {
  var processorInstance = NifiProcessorApi.create(GFFPName, GFFPType, ClientToken)
  JsonUtil.prettyPrint(processorInstance.toJson)
  val gfpid = processorInstance.id

  processorInstance = NifiProcessorApi.create(LAPName, LAPType, ClientToken)
  JsonUtil.prettyPrint(processorInstance.toJson)

  NifiProcessorApi.remove(gfpid, ClientToken)
} catch {
  case e: RESTException =>
    println(e.getErrorResponse.getCode + ":" + e.getErrorResponse.getHttpStatusCode)
}

//NifiProcessorApi.delete("f954970a-857c-4861-9565-790b5a7bd3d2")