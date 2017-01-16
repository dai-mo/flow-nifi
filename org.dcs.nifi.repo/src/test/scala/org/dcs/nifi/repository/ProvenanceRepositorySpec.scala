package org.dcs.nifi.repository

import java.nio.file.{Path, Paths}
import java.util.{Date, UUID}

import org.apache.nifi.provenance.search.{Query, SearchTerm, SearchableField}
import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType, ProvenanceRepository, SearchableFields}
import org.dcs.nifi.{FlowDataProvenance, FlowProvenanceEventRecord}

import scala.collection.JavaConverters._
/**
  * Created by cmathew on 11.01.17.
  */

@Ignore // set to ignore until integration environment is setup
object ProvenanceRepositorySpec {
  val Attributes = "attr1:value1,attr2:value2"
  val PreviousAttributes = "prevattr1:value1,prevattr2:value2"
  val UpdatedAttributes = "updatedvattr1:value1,updatedattr2:value2"
}

class ProvenanceRepositorySpec extends ProvenanceRepositoryBehaviors {

  val nifiPropertiesPath: Path = Paths.get(this.getClass.getResource("nifi.properties").toURI)
  System.setProperty("nifi.properties.file.path", nifiPropertiesPath.toString)


  "Flow Data Provenance using Cassandra" should "should be registered / queried correctly" in {
    val cpr: ProvenanceRepository = new CassandraProvenanceRepository()
    cpr.asInstanceOf[CassandraProvenanceRepository].purge()
    validateProvenance(cpr)
  }
}


trait ProvenanceRepositoryBehaviors extends RepoUnitSpec {
  import ProvenanceRepositorySpec._

  def genFlowDataProvenance(eventType: ProvenanceEventType,
                            componentId: String,
                            flowFileUuid: String,
                            relationship: String): FlowDataProvenance = {
    FlowDataProvenance(UUID.randomUUID().toString,
      0,
      new Date().getTime,
      new Date().getTime,
      new Date().getTime,
      1234,
      1234,
      15,
      eventType.name(),
      Attributes,
      PreviousAttributes,
      UpdatedAttributes,
      componentId,
      "org.dcs.nifi.processor.TestProcessor",
      "",
      "sourceSystemFlowFileIdentifier",
      flowFileUuid,
      UUID.randomUUID().toString + "," +  UUID.randomUUID().toString,
      UUID.randomUUID().toString + "," +  UUID.randomUUID().toString,
      "",
      "details",
      relationship,
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      UUID.randomUUID().toString
    )
  }
  def validateProvenance(proveRepo: ProvenanceRepository) = {
    val maxEventId = proveRepo.getMaxEventId()
    val startEventId: Long = if(maxEventId == null) 0L else maxEventId

    val eventType = ProvenanceEventType.CONTENT_MODIFIED
    var componentId = UUID.randomUUID().toString
    var flowFileUuid = UUID.randomUUID().toString
    var relationship = "success"
    var fdp = genFlowDataProvenance(eventType,
      componentId,
      flowFileUuid,
      relationship)

    var per: ProvenanceEventRecord = fdp.toProvenanceEventRecord()
    proveRepo.registerEvent(per)

    val eventId1 = proveRepo.getMaxEventId
    assert(eventId1 == startEventId + 1)


    val registerdPer = proveRepo.getEvent(eventId1)
    val registeredFdp = FlowProvenanceEventRecord.toFlowDataProvenance(registerdPer, None)
    assert(registeredFdp.eventId == eventId1)
    assert(registeredFdp.attributes == Attributes)
    assert(registeredFdp.previousAttributes == PreviousAttributes)
    assert(registeredFdp.updatedAttributes == UpdatedAttributes)
    assert(registeredFdp.componentId == componentId)
    assert(registeredFdp.flowFileUuid == flowFileUuid)
    assert(registeredFdp.relationship == relationship)
    assert(registeredFdp.eventType == eventType.name())

    componentId = UUID.randomUUID().toString
    flowFileUuid = UUID.randomUUID().toString
    relationship = "success"
    fdp = genFlowDataProvenance(eventType,
      componentId,
      flowFileUuid,
      relationship)
    per = fdp.toProvenanceEventRecord()
    proveRepo.registerEvent(per)

    val eventId2 = proveRepo.getMaxEventId
    assert(eventId2 == startEventId + 2)

    val registerdPers = proveRepo.getEvents(eventId2 - 1, 2, null)

    val registeredFdps = registerdPers.asScala.map(FlowProvenanceEventRecord.toFlowDataProvenance(_, None))
    assert(registeredFdps.head.eventId == eventId1)
    assert(registeredFdps.tail.head.eventId == eventId2)

    val searchQuery = new Query(UUID.randomUUID().toString)
    searchQuery.setMaxResults(2)

    val componentIdSearchTerm = new SearchTerm() {
      override def getValue: String = componentId
      override def getSearchableField: SearchableField = SearchableFields.ComponentID
    }

    searchQuery.addSearchTerm(componentIdSearchTerm)

    var qs = proveRepo.submitQuery(searchQuery, null)
    assert(qs.getResult.getPercentComplete == 0)

    var events = qs.getResult.getMatchingEvents
    assert(events.size == 0)

    qs = proveRepo.retrieveQuerySubmission(qs.getQueryIdentifier, null)
    events = qs.getResult.getMatchingEvents
    assert(events.size == 1)
    assert(events.get(0).getEventId == eventId2)

    val relationshipIdSearchTerm = new SearchTerm() {
      override def getValue: String = relationship
      override def getSearchableField: SearchableField = SearchableFields.Relationship
    }
    searchQuery.addSearchTerm(relationshipIdSearchTerm)

    qs = proveRepo.submitQuery(searchQuery, null)
    qs = proveRepo.retrieveQuerySubmission(qs.getQueryIdentifier, null)
    events = qs.getResult.getMatchingEvents
    assert(events.size == 2)
    assert(events.get(0).getEventId == eventId1)
    assert(events.get(1).getEventId == eventId2)


  }
}