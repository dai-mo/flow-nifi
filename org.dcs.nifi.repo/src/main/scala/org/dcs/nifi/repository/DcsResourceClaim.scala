package org.dcs.nifi.repository

import java.time.Instant
import java.util.{Date, UUID}

import org.apache.nifi.controller.repository.claim.ResourceClaim

/**
  * Created by cmathew on 08.12.16.
  */
class DcsResourceClaim(lossTolerant: Boolean) extends ResourceClaim {

  val createTimestamp = Date.from(Instant.now())
  val uuid = UUID.randomUUID()

  override def getSection: String = ""

  override def getId: String = uuid.toString

  override def isLossTolerant: Boolean = lossTolerant

  override def getContainer: String = ""

  override def compareTo(rc: ResourceClaim): Int = {
    if(rc == null || !rc.isInstanceOf[DcsResourceClaim])
      1
    else {
      val that = rc.asInstanceOf[DcsResourceClaim]
      this.createTimestamp.compareTo(that.createTimestamp)
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[DcsResourceClaim])
      false
    else {
      val that = obj.asInstanceOf[DcsResourceClaim]
      this.uuid == that.uuid && this.createTimestamp.eq(that.createTimestamp)
    }
  }

  override def hashCode(): Int = uuid.hashCode

  override def toString: String = "QuillResourceClaim[id=" + uuid.toString + ", createTimestamp=" + createTimestamp.toString + "]"
}
