package org.dcs.nifi.processors

import java.util

import org.dcs.api.processor.{Configuration, PropertySettings, RelationshipSettings, RemoteProcessor}
/**
  * Created by cmathew on 31/08/16.
  */
class MockRemoteProcessor(remoteProcessor: RemoteProcessor, response: Array[Byte]) extends RemoteProcessor {
  override def schedule(): Boolean = remoteProcessor.schedule()

  override def execute(input: Array[Byte], properties: util.Map[String, String]): AnyRef = remoteProcessor.execute(input, properties)

  override def trigger(input: Array[Byte], properties: util.Map[String, String]): Array[Byte] = response

  override def unschedule(): Boolean = remoteProcessor.unschedule()

  override def stop(): Boolean = remoteProcessor.stop()

  override def shutdown(): Boolean = remoteProcessor.shutdown()

  override def remove(): Boolean = remoteProcessor.remove()

  override def properties(): util.List[PropertySettings] = remoteProcessor.properties

  override def configuration(): Configuration = remoteProcessor.configuration

  override def relationships(): util.Set[RelationshipSettings] = remoteProcessor.relationships
}
