package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.api.service.Provenance
import org.dcs.flow.RestBaseUnitSpec
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
        Matchers.any[List[(String, String)]]
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

    val results = validateProvenanceRetrieval(provenanceClient, ProcessorId)
    assert(results.size == 3)
  }
}


trait ProvenanceApiBehaviours extends RestBaseUnitSpec {
  this: FlatSpec =>

  def validateProvenanceRetrieval(provenanceClient: NifiProvenanceClient, processorId: String): List[Provenance] = {

    val provenanceResults = provenanceClient.provenance(processorId, 10).futureValue
    assert(provenanceResults.size > 0)
    provenanceResults.foreach(r => assert(!r.getContent.isEmpty))
    provenanceResults
  }
}