package org.dcs.nifi.repository

import org.apache.nifi.controller.repository.claim.{ContentClaim, ResourceClaim}

/**
  * Created by cmathew on 08.12.16.
  */
class DcsContentClaim(drc: DcsResourceClaim) extends ContentClaim {

  private var length: Long = -1L

  def setLength(length: Long) = this.length = length

  def getTimestamp = drc.createTimestamp

  override def getLength: Long = length

  override def getOffset: Long = 0

  override def getResourceClaim: ResourceClaim = drc

  override def compareTo(o: ContentClaim): Int = drc.compareTo(o.getResourceClaim)

  override def equals(obj: scala.Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[DcsContentClaim])
      false
    else {
      if(this == obj)
        true
      else {
        val that = obj.asInstanceOf[DcsContentClaim]
        this.getResourceClaim.eq(that.getResourceClaim)
      }
    }
  }

  override def hashCode(): Int = drc.hashCode()

  override def toString: String = "QuillContentClaim[rc_id=" + drc.getId + ", createTimestamp=" + drc.createTimestamp.toString + ", length=" + length + "]"
}
