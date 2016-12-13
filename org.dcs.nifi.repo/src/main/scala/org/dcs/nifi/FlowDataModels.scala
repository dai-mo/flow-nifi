package org.dcs.nifi

/**
  * Created by cmathew on 06.12.16.
  */

import java.util.{Date, UUID}

import org.dcs.nifi.repository.DcsContentClaim


object FlowDataContent {

  def apply(claim: DcsContentClaim, data: Array[Byte]): FlowDataContent = {
    new FlowDataContent(claim.getResourceClaim.getId, 0, claim.getTimestamp, data)
  }
}
case class FlowDataContent(id: String, claimCount: Int, timestamp: Date, data: Array[Byte]) {
  override def equals(that: Any): Boolean = that match {
    case FlowDataContent(thatId, claimCount, thatTimestamp, thatData) =>
      thatId == this.id &&
      claimCount == this.claimCount &&
        thatTimestamp == this.timestamp &&
        thatData.deep == this.data.deep
    case _ => false
  }
}
