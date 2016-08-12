package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiProvenanceApi, NifiProvenanceClient, Provenance}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec

/**
  * Created by cmathew on 12/08/16.
  */

object ProvenanceApiSpec {

}

class ProvenanceApiSpec extends RestBaseUnitSpec with ProvenanceApiBehaviours {

  val ProcessorId = "3fde726d-5cc1-4bb6-1e64-241766adaf86"
  val ProvenanceQueryId = "7bb925a6-0156-1000-7c10-5dacea5eeb4b"

  "Provenance Query" must "produce valid results" in {
    val queryResponsePath: Path = Paths.get(this.getClass.getResource("provenance/submit-provenance-query.json").toURI)
    val provenanceResponse: Path = Paths.get(this.getClass.getResource("provenance/provenance.json").toURI)
    val contentResponsePath: Path = Paths.get(this.getClass.getResource("provenance/output.txt").toURI)

    val provenanceClient = spy(new NifiProvenanceApi)

    doReturn(jsonFromFile(queryResponsePath.toFile)).
      when(provenanceClient).
      postAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath),
        Matchers.any[Form],
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    doReturn(jsonFromFile(provenanceResponse.toFile)).
      when(provenanceClient).
      getAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath + "/" + ProvenanceQueryId),
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]]
      )

    for(eventId <- List("197", "192", "187")) {
      doReturn(jsonFromFile(contentResponsePath.toFile)).
        when(provenanceClient).
        getAsJson(
          Matchers.eq(NifiProvenanceClient.provenanceOutput(eventId)),
          Matchers.any[Map[String, String]],
          Matchers.any[List[(String, String)]]
        )
    }

    doReturn("").
      when(provenanceClient).
      deleteAsJson(
        Matchers.eq(NifiProvenanceClient.ProvenancePath + "/" + ProvenanceQueryId),
        Matchers.any[Map[String, String]],
        Matchers.any[List[(String, String)]]
      )

    val results = validateProvenanceRetrieval(provenanceClient, ProcessorId)
    assert(results.size == 3)
  }
}


trait ProvenanceApiBehaviours {
  this: FlatSpec =>

  def validateProvenanceRetrieval(provenanceClient: NifiProvenanceClient, processorId: String): List[Provenance] = {

    val provenanceResults = provenanceClient.provenance(processorId, 10)

    assert(provenanceResults.size > 0)

    provenanceResults
  }
}