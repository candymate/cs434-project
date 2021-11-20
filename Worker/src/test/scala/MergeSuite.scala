import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class MergeSuite extends AnyFunSuite {

}

@RunWith(classOf[JUnitRunner])
class MultiFileSuite extends AnyFunSuite {
  val sampleFile1 = new File("Worker/src/test/files/sample")
  val sampleFile2 = new File("Worker/src/test/files/sample2")

  test("single file read test") {
    val mf1 = new MultiFile(List(sampleFile1))
    assert(mf1.readOneRecord.key == "AsfAGHM5om")
    assert(mf1.readOneRecord.key == "~sHd0jDv6X")
    assert(mf1.seekRead(0, 500) == (0,500))
    assert(mf1.readOneRecord.key == "*}-Wz1;TD-")
  }

  test("multiple file read test") {
    val mf1 = new MultiFile(List(sampleFile1, sampleFile2))
    assert(mf1.seekRead(0, 900) == (0,900))
    assert(mf1.readOneRecord.key == "5HA\\z%qt{%")
    assert(mf1.readOneRecord.key == "`PkXQ<&+cc")
  }
}
