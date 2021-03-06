/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.nifi.repository

import java.nio.file.{Path, Paths}
import java.util.{Date, UUID}

import org.apache.nifi.provenance.search.{Query, SearchTerm, SearchableField}
import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType, ProvenanceRepository, SearchableFields}
import org.dcs.data.slick.{BigTables, Tables}
import org.dcs.nifi.FlowProvenanceEventRecord
import org.scalatest.Ignore

import scala.collection.JavaConverters._

import org.dcs.api.processor.Attributes

/**
  * Created by cmathew on 11.01.17.
  */


object ProvenanceRepositorySpec {
  val TestAttributes = "attr1:value1,attr2:value2"
  val TestPreviousAttributes = "prevattr1:value1,prevattr2:value2"
  val TestUpdatedAttributes = "updatedvattr1:value1,updatedattr2:value2"
}

@Ignore // set to ignore until integration environment is setup
class ProvenanceRepositorySpec extends ProvenanceRepositoryBehaviors {

  val nifiPropertiesPath: Path = Paths.get(this.getClass.getResource("nifi.properties").toURI)
  System.setProperty("nifi.properties.file.path", nifiPropertiesPath.toString)


  "Flow Data Provenance" should "should be registered / queried correctly" in {
    val cpr: ProvenanceRepository = new SlickPostgresProvenanceRepository
    cpr.asInstanceOf[ManageRepository].purge()
    validateProvenance(cpr)
    cpr.asInstanceOf[ManageRepository].purge()
  }

}


trait ProvenanceRepositoryBehaviors extends RepoUnitSpec {
  import ProvenanceRepositorySpec._

  def genFlowDataProvenance(eventType: ProvenanceEventType,
                            componentId: String,
                            flowFileUuid: String,
                            relationship: String): BigTables.BigFlowDataProvenanceRow = {
    BigTables.BigFlowDataProvenanceRow(UUID.randomUUID().toString,
      0,
      Option(new Date().getTime),
      Option(new Date().getTime),
      Option(new Date().getTime),
      Option(1234),
      Option(1234),
      Option(15),
      Option(eventType.name()),
      Option(TestAttributes),
      Option(TestPreviousAttributes),
      Option(Attributes.RelationshipAttributeKey + ":" + relationship + "," + TestUpdatedAttributes),
      Option(componentId),
      Option("org.dcs.nifi.processor.TestProcessor"),
      Option(""),
      Option("sourceSystemFlowFileIdentifier"),
      Option(flowFileUuid),
      Option(UUID.randomUUID().toString + ")," +  UUID.randomUUID().toString),
      Option(UUID.randomUUID().toString + ")," +  UUID.randomUUID().toString),
      Option(""),
      Option("details"),
      Option(null),
      Option(UUID.randomUUID().toString),
      Option(UUID.randomUUID().toString),
      Option(UUID.randomUUID().toString)
    )
  }
  def validateProvenance(proveRepo: ProvenanceRepository) = {


    val eventType = ProvenanceEventType.CONTENT_MODIFIED
    var componentId = UUID.randomUUID().toString
    var flowFileUuid = UUID.randomUUID().toString
    var relationship = "success"
    var fdp = genFlowDataProvenance(eventType,
      componentId,
      flowFileUuid,
      relationship)

    var per: ProvenanceEventRecord = FlowProvenanceEventRecord(fdp)
    proveRepo.registerEvent(per)

    val maxEventId = proveRepo.getMaxEventId
    val startEventId: Long = if(maxEventId == null) 0L else maxEventId


    val registerdPer = proveRepo.getEvent(startEventId)
    val registeredFdp = FlowProvenanceEventRecord.toFlowDataProvenance(registerdPer, None)
    assert(registeredFdp.eventId == startEventId)
    assert(registeredFdp.attributes == TestAttributes)
    assert(registeredFdp.previousAttributes == TestPreviousAttributes)
    assert(registeredFdp.updatedAttributes == TestUpdatedAttributes)
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
    per = FlowProvenanceEventRecord(fdp)
    proveRepo.registerEvent(per)

    val eventId1 = proveRepo.getMaxEventId
    assert(eventId1 == startEventId + 1)

    val registerdPers = proveRepo.getEvents(eventId1 - 1, 2, null)

    val registeredFdps = registerdPers.asScala.map(FlowProvenanceEventRecord.toFlowDataProvenance(_, None))
    assert(registeredFdps.head.eventId == startEventId)
    assert(registeredFdps.tail.head.eventId == eventId1)

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
    assert(events.get(0).getEventId == eventId1)

    val relationshipIdSearchTerm = new SearchTerm() {
      override def getValue: String = relationship
      override def getSearchableField: SearchableField = SearchableFields.Relationship
    }
    searchQuery.addSearchTerm(relationshipIdSearchTerm)

    qs = proveRepo.submitQuery(searchQuery, null)
    qs = proveRepo.retrieveQuerySubmission(qs.getQueryIdentifier, null)
    events = qs.getResult.getMatchingEvents
    assert(events.size == 2)
    assert(events.get(0).getEventId == startEventId)
    assert(events.get(1).getEventId == eventId1)


  }
}