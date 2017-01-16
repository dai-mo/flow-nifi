package org.dcs.nifi.repository

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.file.Path
import java.util

import io.getquill._
import org.apache.nifi.controller.repository.ContentRepository
import org.apache.nifi.controller.repository.claim.{ContentClaim, ResourceClaimManager}
import org.apache.nifi.stream.io.StreamUtils
import org.dcs.nifi.FlowDataContent

import scala.collection.JavaConverters._

/**
  * FIXME: This class is duplicated for all other Content Repository
  * implementations using Quill. This is done as a workaround until
  * the implementation of https://github.com/getquill/quill/issues/379
  * which will allow for the possibility of choosing the target context
  * at runtime. This means that the diff between all the implementations
  * should be only the class name and the ''val ctx = ....'' line
  *
  * Created by cmathew on 05.12.16.
  */

class CassandraContentRepository extends ContentRepository {

  val ctx = new CassandraSyncContext[SnakeCase]("cassandra")


  def getContentRecord(contentClaim: ContentClaim): Option[FlowDataContent] = {
    import ctx._

    val dataQuery = quote(query[FlowDataContent].filter(p => p.id == lift(contentClaim.getResourceClaim.getId)))
    val result = ctx.run(dataQuery)

    if(result.size > 1)
      throw new IllegalStateException("More than one content record for given id " + contentClaim.getResourceClaim.getId)
    result.headOption

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
    ctx.close()
  }

  override def importFrom(content: Path, claim: ContentClaim): Long = throw new UnsupportedOperationException()

  override def importFrom(content: InputStream, claim: ContentClaim): Long = throw new UnsupportedOperationException()

  override def cleanup(): Unit = {}

  override def getContainerUsableSpace(containerName: String): Long = 0

  override def incrementClaimaintCount(claim: ContentClaim): Int = {
    import ctx._

    val content = getContentRecord(claim)
    if (content.isDefined) {
      val count = content.get.claimCount + 1

      val claimCountUpdate = quote {
        query[FlowDataContent]
          .filter(p => p.id == lift(claim.getResourceClaim.getId))
          .update(_.claimCount -> lift(count))
      }
      ctx.run(claimCountUpdate)
      count
    } else
      0
  }

  override def decrementClaimantCount(claim: ContentClaim): Int = {
    import ctx._

    val content = getContentRecord(claim)
    if (content.isDefined) {
      val count = content.get.claimCount - 1

      val claimCountUpdate = quote {
        query[FlowDataContent]
          .filter(p => p.id == lift(claim.getResourceClaim.getId))
          .update(_.claimCount -> lift(count))
      }
      ctx.run(claimCountUpdate)
      count
    } else
      0
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

    if(content.isEmpty)
      throw new IllegalStateException("Cannot clone content claim with no claim")

    val resourceClaim = new DcsResourceClaim(lossTolerant)
    val contentClaim = new DcsContentClaim(resourceClaim)

    import ctx._

    val contentClone = quote(query[FlowDataContent]
      .insert(lift(FlowDataContent(contentClaim, content.get.data))))
    ctx.run(contentClone)

    contentClaim.setLength(content.get.data.length)
    contentClaim
  }

  override def initialize(claimManager: ResourceClaimManager): Unit = {}

  override def remove(claim: ContentClaim): Boolean = {
    import ctx._

    val contentDelete = quote {
      query[FlowDataContent]
        .filter(p => p.id == lift(claim.getResourceClaim.getId))
        .delete
    }
    ctx.run(contentDelete)
    true
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
    getContentRecord(claim).map(fdc => fdc.claimCount).getOrElse(0)

  override def read(claim: ContentClaim): InputStream = {
    val flowDataContent = getContentRecord(claim)
    if(flowDataContent.isEmpty)
      return new ByteArrayInputStream(Array.empty[Byte])
    else {
      new ByteArrayInputStream(flowDataContent.get.data)
    }
  }

  override def getContainerCapacity(containerName: String): Long = 0

  override def getContainerNames: util.Set[String] = new util.HashSet[String]()

  override def create(lossTolerant: Boolean): ContentClaim = {

    val resourceClaim = new DcsResourceClaim(lossTolerant)
    val contentClaim = new DcsContentClaim(resourceClaim)

    import ctx._

    val contentCreate = quote(query[FlowDataContent]
      .insert(lift(FlowDataContent(contentClaim, Array.empty[Byte]))))
    ctx.run(contentCreate)

    contentClaim
  }

  override def purge(): Unit = {
    import ctx._

    val contentPurge = quote {
      query[FlowDataContent]
        .delete
    }
    ctx.run(contentPurge)
  }

  class QuillOutputStream(claim: ContentClaim) extends OutputStream {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()

    override def write(b: Int): Unit = {
      out.write(b)
    }

    override def flush(): Unit = {
      out.flush()

      import ctx._

      val bytes = out.toByteArray
      val dataUpdate = quote {
        query[FlowDataContent]
          .filter(p => p.id == lift(claim.getResourceClaim.getId))
          .update(_.data -> lift(bytes), _.claimCount -> 0)
      }
      ctx.run(dataUpdate)

      claim.asInstanceOf[DcsContentClaim].setLength(bytes.length)
    }

    override def close() = {
      out.close()
    }
  }

}





