package org.dcs.flow

import java.io.{File, IOException}
import javax.ws.rs.client.{ClientRequestContext, ClientRequestFilter}

import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.junit.JUnitSuite
import org.scalatest.FlatSpec
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.slf4j.LoggerFactory


trait FlowTestUtil {

  def jsonFromFile(jsonFile: File): String = {
    val source = scala.io.Source.fromFile(jsonFile)
    try source.mkString finally source.close()
  }
}



trait FlowBaseUnitSpec extends Matchers
  with OptionValues
  with Inside
  with Inspectors
  with MockitoSugar
  with FlowTestUtil
  with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  // creates timeout in seconds for futures
  def timeout(secs: Int) =
    Timeout(Span(secs, Seconds))

}

abstract class FlowUnitSpec  extends FlatSpec
  with FlowBaseUnitSpec
  with BeforeAndAfterEach
  with BeforeAndAfter
  with BeforeAndAfterAll

abstract class AsyncFlowUnitSpec extends AsyncFlatSpec
  with FlowBaseUnitSpec
  with BeforeAndAfterEach
  with BeforeAndAfter
  with BeforeAndAfterAll

// FIXME: Currently the only way to use the mockito
// inject mock mechanism to test the CDI
// part is to run the test as JUnit tests
// since there is no mechanism to run this
// as a scala test.
// ScalaMock could be an option once the 
// issue https://github.com/paulbutcher/ScalaMock/issues/100
// is resolved
abstract class JUnitSpec extends JUnitSuite
  with Matchers
  with OptionValues
  with Inside
  with Inspectors
  with MockitoSugar
  with FlowTestUtil

object IT extends Tag("IT")

