import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.security.MessageDigest
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class MergeSuite extends AnyFunSuite {
    // https://alvinalexander.com/scala/how-to-list-files-in-directory-filter-names-scala/
    def getListOfFiles(dir: String): List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList.sorted
        } else {
            List[File]()
        }
    }

    ignore("merge test") {
        val fileList = getListOfFiles("/tmp/test")
        val mfl = fileList.map(f => new MultiFileRead(List(f)))
        MergeUtil.mergeFiles(new File("/tmp/test"), mfl)
    }
}

@RunWith(classOf[JUnitRunner])
class MultiFileSuite extends AnyFunSuite {
    // https://stackoverflow.com/questions/38855843/scala-one-liner-to-generate-md5-hash-from-string
    def md5sum(inputStr: String): String = {
        val md: MessageDigest = MessageDigest.getInstance("MD5")
        md.digest(inputStr.getBytes()).map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

    val sampleReadFile1 = new File("Worker/src/test/files/merge/sample")
    val sampleReadFile2 = new File("Worker/src/test/files/merge/sample2")

    test("single file read test") {
        val mf1 = new MultiFileRead(List(sampleReadFile1))
        assert(mf1.readOneRecord.get.key == "AsfAGHM5om")
        assert(mf1.readOneRecord.get.key == "~sHd0jDv6X")
        mf1.close()
    }

    test("multiple file length test") {
        val mf1 = new MultiFileRead(List(sampleReadFile1, sampleReadFile2))
        assert(mf1.length == 2000)
        mf1.close()
    }

    test("record write test") {
        val mf1 = new MultiFileWrite("Worker/src/test/files/merge")
        mf1.writeOneRecord(new Record("ABCDEFGHIJ", "  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\r\n"))
        mf1.writeOneRecord(new Record("BCDEFGHIJK", "  00000000000000000000000000000000  0000222200002222000022220313222200002222000000001111\r\n"))
        mf1.close()
        assert(md5sum(Source.fromFile(mf1.getFileList()(0).getAbsolutePath()).mkString) == "e12795094960457f6333520ec4f8dbc0")
        mf1.getFileList()(0).delete()
    }

    ignore("write and rename multiple files test") {
        val mf1 = new MultiFileWrite("Worker/src/test/files/merge")
        (1 to 400000).foreach(x => mf1.writeOneRecord(new Record("ABCDEFGHIJ", "  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\r\n")))
        mf1.close()

        assert(md5sum(Source.fromFile(mf1.getFileList()(0).getAbsolutePath()).mkString) == "ac7ec3cef9e3bd8e05400e0eb9040a32")
        assert(md5sum(Source.fromFile(mf1.getFileList()(1).getAbsolutePath()).mkString) == "af53f02ccc505202fba4cdc612fe3707")

        val prevFiles = mf1.getFileList
        assert(mf1.renameFiles(List(new File("Worker/src/test/files/merge/rename1"),
            new File("Worker/src/test/files/merge/rename2"))))
        assert(!prevFiles(0).exists && !prevFiles(1).exists)
        assert(mf1.getFileList()(0).exists && mf1.getFileList()(1).exists)

        mf1.getFileList()(0).delete()
        mf1.getFileList()(1).delete()
    }

    test("remove multiple files test") {
        val fileList = List(new File("Worker/src/test/files/merge/remove1"),
            new File("Worker/src/test/files/merge/remove2"))
        val mf1 = new MultiFileRead(fileList)

        assert(fileList(0).createNewFile() && fileList(1).createNewFile())
        mf1.removeFiles()
        assert(!fileList(0).exists && !fileList(1).exists)
        mf1.close()
    }
}
