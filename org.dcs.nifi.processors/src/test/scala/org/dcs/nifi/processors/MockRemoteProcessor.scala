package org.dcs.nifi.processors

import java.util

import org.dcs.api.processor._
import org.dcs.api.service.RemoteProcessorService
/**
  * Created by cmathew on 31/08/16.
  */
class MockRemoteProcessorService(processor: RemoteProcessor, response: Array[Byte]) extends RemoteProcessorService {

  override def execute(input: Array[Byte], properties: util.Map[String, String]): AnyRef = processor.execute(input, properties)

  override def trigger(input: Array[Byte], properties: util.Map[String, String]): Array[Byte] = response

  override def relationships(): util.Set[RemoteRelationship] = processor.relationships()

  override def properties(): util.List[RemoteProperty] = processor.properties()

  override def configuration: Configuration = processor.configuration

  override def metadata(): MetaData = processor.metadata

  override def getDef(processor: RemoteProcessor): ProcessorDefinition = processor

  override def initialise(): RemoteProcessor = processor
}
