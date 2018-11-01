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

package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.MediaType

import org.dcs.api.service.Provenance
import org.dcs.flow.{FlowBaseUnitSpec, FlowUnitSpec}
import org.dcs.flow.nifi.{NifiProvenanceApi, NifiProvenanceClient}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec

import scala.concurrent.Future

/**
  * Created by cmathew on 12/08/16.
  */

object ProvenanceApiSpec {

}

class ProvenanceApiSpec extends ProvenanceApiBehaviours {

  val ProcessorId = "3fde726d-5cc1-4bb6-1e64-241766adaf86"
  val ProvenanceQueryId = "7bb925a6-0156-1000-7c10-5dacea5eeb4b"

  "Provenance Query" must "produce valid results" in {
    val queryResponsePath: Path = Paths.get(this.getClass.getResource("provenance/submit-provenance-query.json").toURI)
    val provenanceResponse: Path = Paths.get(this.getClass.getResource("provenance/provenance.json").toURI)
    val contentResponsePath: Path = Paths.get(this.getClass.getResource("provenance/output.txt").toURI)

    val provenanceClient = spy(new NifiProvenanceApi)

    doReturn(Future.successful(jsonFromFile(queryResponsePath.toFile)))
      .when(provenanceClient)
      .postAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath),
        Matchers.any[AnyRef],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    doReturn(Future.successful(jsonFromFile(provenanceResponse.toFile)))
      .when(provenanceClient)
      .getAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath + "/" + ProvenanceQueryId),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    for(eventId <- List("197", "192", "187")) {
      doReturn(Future.successful(jsonFromFile(contentResponsePath.toFile)))
        .when(provenanceClient)
        .getAsJson(
          Matchers.eq(NifiProvenanceClient.provenanceOutput(eventId)),
          Matchers.any[List[(String, String)]],
          Matchers.any[List[(String, String)]]
        )
    }

    doReturn(Future.successful(""))
      .when(provenanceClient)
      .deleteAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath + "/" + ProvenanceQueryId),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )
    // FIXME: This test needs to be adapted to the avro serde
//    val results = validateProvenanceRetrieval(provenanceClient, ProcessorId)
//    assert(results.size == 3)
  }
}


trait ProvenanceApiBehaviours extends FlowUnitSpec {
  this: FlatSpec =>

  // FIXME: This test needs to be adapted to the avro serde
//  def validateProvenanceRetrieval(provenanceClient: NifiProvenanceClient, processorId: String): List[Provenance] = {
//    val provenanceResults = provenanceClient.provenance(processorId, 10).futureValue
//    assert(provenanceResults.size > 0)
//    provenanceResults.foreach(r => assert(!r.getContent.isEmpty))
//    provenanceResults
//  }
}