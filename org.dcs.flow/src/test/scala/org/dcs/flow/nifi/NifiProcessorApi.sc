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

val GFFPName = "GenerateFlowFile"
val GFFPType = "org.apache.nifi.processors.standard.GenerateFlowFile"

val LAPName = "LogAttribute"
val LAPType = "org.apache.nifi.processors.standard.LogAttribute"
try {
  var processorInstance = NifiProcessorApi.create(GFFPName, GFFPType)
  JsonUtil.prettyPrint(processorInstance.toJson)

  processorInstance = NifiProcessorApi.create(LAPName, LAPType)
  JsonUtil.prettyPrint(processorInstance.toJson)

} catch {
  case e: RESTException =>
    println(e.getErrorResponse.getCode + ":" + e.getErrorResponse.getHttpStatusCode)
}