package org.dcs.nifi.repository

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.file.Path
import java.sql.Timestamp
import java.util

import org.apache.nifi.controller.repository.ContentRepository
import org.apache.nifi.controller.repository.claim.{ContentClaim, ResourceClaimManager}
import org.apache.nifi.stream.io.StreamUtils
//import org.dcs.api.data.FlowDataContent
import org.dcs.data.IntermediateResultsAdapter
import org.dcs.data.slick.Tables

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by cmathew on 05.12.16.
  */

class BaseContentRepository(ira: IntermediateResultsAdapter) extends ContentRepository {

  private val timeout = 60 seconds

  def getContentRecord(contentClaim: ContentClaim): Option[Tables.FlowDataContentRow] = {
    Await.result(ira.getContent(contentClaim.getResourceClaim.getId), timeout)
  }


  override def isAccessible(contentClaim: ContentClaim): Boolean = {
    if(contentClaim.getResourceClaim.getId.isEmpty)
      false
    else
      getContentRecord(contentClaim).isDefined
  }

  override def exportTo(claim: ContentClaim, destination: Path, append: Boolean): Long = throw new UnsupportedOperationException()

  override def exportTo(claim: ContentClaim, destination: Path, append: Boolean, offset: Long, length: Long): Long = throw new UnsupportedOperationException()

  override def exportTo(claim: ContentClaim, destination: OutputStream): Long = throw new UnsupportedOperationException()

  override def exportTo(claim: ContentClaim, destination: OutputStream, offset: Long, length: Long): Long = throw new UnsupportedOperationException()

  override def shutdown(): Unit = {
    // FIXME: Add code to close connection
  }

  override def importFrom(content: Path, claim: ContentClaim): Long = throw new UnsupportedOperationException()

  override def importFrom(content: InputStream, claim: ContentClaim): Long = throw new UnsupportedOperationException()

  override def cleanup(): Unit = {}

  override def getContainerUsableSpace(containerName: String): Long = 0

  override def incrementClaimaintCount(claim: ContentClaim): Int = {
    Await.result(ira.incrementClaimaintCount(claim.getResourceClaim.getId), timeout).getOrElse(-1)
  }

  override def decrementClaimantCount(claim: ContentClaim): Int = {
    Await.result(ira.decrementClaimaintCount(claim.getResourceClaim.getId), timeout).getOrElse(-1)
  }


  override def merge(claims: util.Collection[ContentClaim],
                     destination: ContentClaim,
                     header: Array[Byte],
                     footer: Array[Byte],
                     demarcator: Array[Byte]): Long = {
    if (claims.contains(destination))
      throw new IllegalArgumentException("Destination cannot be one of the claims to be merged")

    val out = write(destination)
    if (header != null) {
      out.write(header);
    }
    val scClaims = claims.asScala
    scClaims.foreach(claim => {
      if(scClaims.head != claim)
        out.write(demarcator)
      StreamUtils.copy(read(claim), out)
    })

    if (footer != null) {
      out.write(footer)
    }
    out.flush()
    destination.getLength
  }

  override def size(claim: ContentClaim): Long = {
    if (claim == null)
      return 0L


    claim.getLength
  }

  override def clone(original: ContentClaim, lossTolerant: Boolean): ContentClaim = {
    val content = getContentRecord(original)
    val data = content.get.data.getOrElse(Array.empty[Byte])

    if(content.isEmpty)
      throw new IllegalStateException("Cannot clone content claim with no claim")

    val resourceClaim = new DcsResourceClaim(lossTolerant)
    val contentClaim = new DcsContentClaim(resourceClaim)

    Await.result(ira.createContent(Tables.FlowDataContentRow(contentClaim.getResourceClaim.getId,
      Some(0),
      Some(new Timestamp(contentClaim.getTimestamp.getTime)),
      Some(data))),
      timeout)


    contentClaim.setLength(data.length)
    contentClaim
  }

  override def initialize(claimManager: ResourceClaimManager): Unit = {}

  override def remove(claim: ContentClaim): Boolean = {
    Await.result(ira.deleteContent(claim.getResourceClaim.getId), timeout) match {
      case 0 => false
      case 1 => true
    }
  }

  override def write(claim: ContentClaim): OutputStream = {
    val flowDataContent = getContentRecord(claim)
    if(flowDataContent.isEmpty)
      throw new IllegalStateException("Cannot write to invalid content claim")
    else {
      new QuillOutputStream(claim)
    }
  }

  override def getClaimantCount(claim: ContentClaim): Int =
    Await.result(ira.getClaimantCount(claim.getResourceClaim.getId), timeout).getOrElse(-1)

  override def read(claim: ContentClaim): InputStream = {
    val flowDataContent = getContentRecord(claim)
    if(flowDataContent.isEmpty)
      return new ByteArrayInputStream(Array.empty[Byte])
    else {
      new ByteArrayInputStream(flowDataContent.get.data.getOrElse(Array.empty[Byte]))
    }
  }

  override def getContainerCapacity(containerName: String): Long = 0

  override def getContainerNames: util.Set[String] = new util.HashSet[String]()

  override def create(lossTolerant: Boolean): ContentClaim = {

    val resourceClaim = new DcsResourceClaim(lossTolerant)
    val contentClaim = new DcsContentClaim(resourceClaim)

    Await.result(ira.createContent(Tables.FlowDataContentRow(contentClaim.getResourceClaim.getId,
      Some(0),
      Some(new Timestamp(contentClaim.getTimestamp.getTime)),
      Some(Array.empty[Byte]))),
      timeout)

    contentClaim
  }

  override def purge(): Unit = {
    Await.result(ira.purgeContent(), timeout)
  }

  class QuillOutputStream(claim: ContentClaim) extends OutputStream {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()

    override def write(b: Int): Unit = {
      out.write(b)
    }

    override def flush(): Unit = {
      out.flush()
      val bytes = out.toByteArray
      Await.result(ira.updateDataContent(claim.getResourceClaim.getId, bytes), timeout)
      claim.asInstanceOf[DcsContentClaim].setLength(bytes.length)
    }

    override def close() = {
      out.close()
    }
  }

}





