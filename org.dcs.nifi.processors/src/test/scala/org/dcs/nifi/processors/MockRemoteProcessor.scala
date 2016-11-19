package org.dcs.nifi.processors

import java.util
import java.util.{UUID, List => JavaList}

import org.apache.avro.generic.GenericRecord
import org.dcs.api.processor._
import org.dcs.api.service.{RemoteProcessorService, StatefulRemoteProcessorService}
import org.dcs.commons.error.ErrorResponse
import org.dcs.core.state.LocalStateManager

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 31/08/16.
  */
class MockRemoteProcessorService(processor: RemoteProcessor, response: Array[Array[Byte]])
  extends RemoteProcessorService {

  override def execute(record: Option[GenericRecord], properties: util.Map[String, String]): List[Either[ErrorResponse, AnyRef]] =
    processor.execute(record, properties)

  override def trigger(input: Array[Byte], properties: util.Map[String, String]): Array[Array[Byte]] = {
    processor.trigger(input, properties)
  }

  override def relationships(): util.Set[RemoteRelationship] = processor.relationships()

  override def properties(): util.List[RemoteProperty] = processor.properties()

  override def configuration: Configuration = processor.configuration

  override def metadata(): MetaData = processor.metadata

  override def getDef(processor: RemoteProcessor): ProcessorDefinition = processor

  override def initialise(): RemoteProcessor = processor
}


class MockStatefulRemoteProcessorService(processor: StatefulRemoteProcessor,
                                         response: Array[Array[Byte]])
  extends MockRemoteProcessorService(processor, response)
    with StatefulRemoteProcessorService
    with LocalStateManager {
  override def init(): String = put(processor)

  override def instanceTrigger(processorStateId: String, input: Array[Byte],
                               properties: util.Map[String, String]): Array[Array[Byte]] =
    get(processorStateId).get.trigger(input, properties)
}
