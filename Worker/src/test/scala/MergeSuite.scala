import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.security.MessageDigest
import java.io.File
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class MergeSuite extends AnyFunSuite {

}

@RunWith(classOf[JUnitRunner])
class MultiFileSuite extends AnyFunSuite {
  // https://stackoverflow.com/questions/38855843/scala-one-liner-to-generate-md5-hash-from-string
  def md5sum(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
  }

  val sampleReadFile1 = new File("Worker/src/test/files/merge/sample")
  val sampleReadFile2 = new File("Worker/src/test/files/merge/sample2")

  test("single file read test") {
    val mf1 = new MultiFileRead(List(sampleReadFile1))
    assert(mf1.readOneRecord.key == "AsfAGHM5om")
    assert(mf1.readOneRecord.key == "~sHd0jDv6X")
    assert(mf1.seekRead(0, 500) == (0,500))
    assert(mf1.readOneRecord.key == "*}-Wz1;TD-")
  }

  test("multiple file read test") {
    val mf1 = new MultiFileRead(List(sampleReadFile1, sampleReadFile2))
    assert(mf1.seekRead(0, 900) == (0,900))
    assert(mf1.readOneRecord.key == "5HA\\z%qt{%")
    assert(mf1.readOneRecord.key == "`PkXQ<&+cc")
  }

  test("multiple file length test") {
    val mf1 = new MultiFileRead(List(sampleReadFile1, sampleReadFile2))
    assert(mf1.length == 2000)
  }

  test("record write test") {
    val mf1 = new MultiFileWrite("Worker/src/test/files/merge")
    mf1.writeOneRecord(new Record("ABCDEFGHIJ", "  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\r\n"))
    mf1.writeOneRecord(new Record("BCDEFGHIJK", "  00000000000000000000000000000000  0000222200002222000022220313222200002222000000001111\r\n"))
    assert(md5sum(Source.fromFile(mf1.getFileList()(0).getAbsolutePath()).mkString) == "e12795094960457f6333520ec4f8dbc0")
    mf1.getFileList()(0).delete()
  }

  test("write multiple files test") {
    val mf1 = new MultiFileWrite("Worker/src/test/files/merge")
    (1 to 400000).foreach(x => mf1.writeOneRecord(new Record("ABCDEFGHIJ", "  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\r\n")))
    assert(md5sum(Source.fromFile(mf1.getFileList()(0).getAbsolutePath()).mkString) == "ac7ec3cef9e3bd8e05400e0eb9040a32")
    assert(md5sum(Source.fromFile(mf1.getFileList()(1).getAbsolutePath()).mkString) == "af53f02ccc505202fba4cdc612fe3707")
    mf1.getFileList()(0).delete()
    mf1.getFileList()(1).delete()
  }
}
