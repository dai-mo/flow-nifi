package org.dcs.nifi.rest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.nifi.web.api.entity.ProcessorTypesEntity
import org.dcs.commons.JsonSerializerImplicits._
import org.apache.nifi.web.api.dto.DocumentedTypeDTO
import scala.collection.JavaConverters._
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.Files
import java.io.File
import javax.ws.rs.core.Response
import org.mockito.Mockito._
import scala.io.Source._
import org.mockito.Mockito
import org.scalatest.FlatSpec


object RestApiSpec {
  object NifiProcessorClient extends ProcessorClient with NifiProcessorApi
}

class RestApiSpec extends RestBaseUnitSpec with RestApiBehaviors {

  import RestApiSpec._

  def jsonFromFile(jsonFile: File): String = {
    val source = scala.io.Source.fromFile(jsonFile)
    try source.mkString finally source.close()
  }

  def mockResponse(jsonFile: File) = {
    val response = mock[Response]
    when(response.readEntity(classOf[String])).thenReturn(jsonFromFile(jsonFile))
    response
  }

  val typesPath: Path = Paths.get(this.getClass().getResource("types.json").toURI())
  val processorApi = Mockito.spy(NifiProcessorClient)
  doReturn(mockResponse(typesPath.toFile)).when(processorApi).response(NifiProcessorApi.TypesPath)

  "Processor Types" must " be valid " in {
    validateProcessorTypes(processorApi)
  }

}

trait RestApiBehaviors { this: FlatSpec =>

  val logger: Logger = LoggerFactory.getLogger(classOf[RestApiSpec])

  def validateProcessorTypes(processorApi: ProcessorApi) {    
      val types = processorApi.types
      assert(types.size == 135)    
  }
}