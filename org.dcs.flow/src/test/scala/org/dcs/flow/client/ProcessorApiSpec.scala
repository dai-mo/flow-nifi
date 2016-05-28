package org.dcs.flow.client

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.dcs.commons.JsonSerializerImplicits._
import scala.collection.JavaConverters._
import java.nio.file.Paths
import java.nio.file.Path
import java.io.File
import javax.ws.rs.core.Response
import org.mockito.Mockito._
import scala.io.Source._
import org.mockito.Mockito
import org.scalatest.FlatSpec
import org.dcs.flow.nifi.NifiProcessorApi
import org.dcs.flow.nifi.NifiApiConfig
import org.dcs.flow.ProcessorApi
import org.dcs.flow.client.ProcessorClient
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.BaseRestApi




object ProcessorApiSpec {
  class NifiProcessorClient extends ProcessorClient 
    with NifiProcessorApi 
    with NifiApiConfig 
}

class ProcessorApiSpec extends RestBaseUnitSpec with ProcessorApiBehaviors {

  import ProcessorApiSpec._

  def jsonFromFile(jsonFile: File): String = {
    val source = scala.io.Source.fromFile(jsonFile)
    try source.mkString finally source.close()
  }

  val typesPath: Path = Paths.get(this.getClass().getResource("types.json").toURI())
  val processorApi = Mockito.spy(new NifiProcessorClient())
  doReturn(jsonFromFile(typesPath.toFile)).when(processorApi).getAsJson(NifiProcessorApi.TypesPath, None)
  
  "Processor Types" must " be valid " in {
    validateProcessorTypes(processorApi)
  }
}

trait ProcessorApiBehaviors { this: FlatSpec =>

  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])

  def validateProcessorTypes(processorApi: ProcessorApi) {    
      val types = processorApi.types
      assert(types.size == 135)    
  }
}