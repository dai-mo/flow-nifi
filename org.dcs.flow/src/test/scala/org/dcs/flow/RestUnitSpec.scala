package org.dcs.flow

import java.io.File

import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.scalatest.junit.JUnitSuite
import org.scalatest.FlatSpec


trait FlowTestUtil {

  def jsonFromFile(jsonFile: File): String = {
    val source = scala.io.Source.fromFile(jsonFile)
    try source.mkString finally source.close()
  }
}

abstract class RestBaseUnitSpec extends FlatSpec
  with Matchers
  with OptionValues
  with Inside
  with Inspectors
  with BeforeAndAfterEach
  with BeforeAndAfter
  with MockitoSugar
  with FlowTestUtil

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

