import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Form

import org.apache.nifi.web.api.entity.{Entity, FlowSnippetEntity, TemplatesEntity}
import org.dcs.commons.JsonUtil
import org.dcs.flow.BaseRestApi
import org.dcs.flow.nifi.NifiApiConfig
import org.dcs.commons.JsonSerializerImplicits._
import org.glassfish.jersey.filter.LoggingFilter

object NifiApi extends BaseRestApi with NifiApiConfig

val TemplatesPath = "/controller/templates"
val RevisionPath = "/controller/revision"
val ProcessGroupsBasePath = "/controller/process-groups"
val processGroupId = "root"
val RootProcessGroupsPath = ProcessGroupsBasePath + "/" + processGroupId
val TemplateInstancePath = RootProcessGroupsPath + "/template-instance"

def clientIdVersion(): (String, String) = {
  val revision = NifiApi.getAsJson(RevisionPath)
  val entity = revision.toObject[Entity]()
  (entity.getRevision.getClientId, entity.getRevision.getVersion.toString)
}

def printRequest(api: BaseRestApi, path:String, queryParams: Map[String, String]): Unit= {

  val sb:StringBuilder = StringBuilder.newBuilder
    sb.append(api.baseUrl())
    sb.append(path)
    sb.append("?")
    sb.append(queryParams.map(qp => "&" + qp._1 + "=" + qp._2).mkString.tail)

  println(sb)
}

NifiApi.requestFilter(new LoggingFilter())

val templates = NifiApi.getAsJson(TemplatesPath)

JsonUtil.prettyPrint(templates)

//val templatesDto = templates.toObject[TemplatesEntity]()

//templatesDto.getTemplates

val templateId = "d615fb63-bc39-458c-bfcf-1f197ecdc817"

val (clientId, version) = clientIdVersion()

val tiqp = Map(
  "version" -> version,
  //"clientId" -> clientId,
  "templateId" -> templateId,
  "originX" -> "17",
  "originY" -> "100"
)

val headers = Map(
  "Accept" -> "application/json, text/javascript, */*; q=0.01",
  "Content-Type" -> "application/x-www-form-urlencoded; charset=UTF-8"
)

printRequest(NifiApi, TemplateInstancePath, tiqp)

val template = NifiApi.postAsJson(path = TemplateInstancePath,
  queryParams = tiqp,
  obj = new Form(),
  contentType = MediaType.APPLICATION_FORM_URLENCODED
)

JsonUtil.prettyPrint(template)



