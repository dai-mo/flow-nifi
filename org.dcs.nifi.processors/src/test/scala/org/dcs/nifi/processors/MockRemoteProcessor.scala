package org.dcs.nifi.processors

import java.util

import org.dcs.api.processor._
/**
  * Created by cmathew on 31/08/16.
  */
class MockRemoteProcessor(remoteProcessor: RemoteProcessor, response: Array[Byte]) extends RemoteProcessor {

  override def execute(input: Array[Byte], properties: util.Map[String, String]): AnyRef = remoteProcessor.execute(input, properties)

  override def trigger(input: Array[Byte], properties: util.Map[String, String]): Array[Byte] = response

  override def relationships(): util.Set[RemoteRelationship] = remoteProcessor.relationships()

  override def properties(): util.List[RemoteProperty] = remoteProcessor.properties()

  override def configuration: Configuration = remoteProcessor.configuration

  override def metadata(): MetaData = remoteProcessor.metadata
}
