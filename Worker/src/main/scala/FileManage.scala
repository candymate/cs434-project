import org.slf4j.LoggerFactory

import java.io._
import scala.util.Try
import java.io.{BufferedWriter, File, FileWriter, IOException, BufferedReader, FileReader}

class Record(val key: String, val value: String) {
    override def toString: String = {
        "Key: " + key + " / Value: " + value
    }
}

class MultiFileRead(private[this] var fileList: List[File]) {
    val log = LoggerFactory.getLogger(getClass)

    private[this] var fileIdx, fileOff = 0

    private[this] var fstream: BufferedReader = null

    def length(): Int = {
        assert(fileList != Nil)
        fileList.foldLeft(0)((acc, f) => acc + f.length.toInt)
    }

    def readOneRecord(): Option[Record] = {
        assert(fileList != Nil)

        if (fileIdx >= fileList.size) {
            return None
        }

        if (fstream == null) {
            fstream = new BufferedReader(new FileReader(fileList(fileIdx)))
        }

        val line = fstream.readLine()
        if (line.size == 100 - 2) {
            fileOff += line.size + 2 // gensort uses \r\n
            if (fileOff >= fileList(fileIdx).length()) {
                fileOff -= fileList(fileIdx).length().toInt
                fileIdx += 1
                fstream.close()
                fstream = null
            }
            val rec = new Record(line.slice(0, 10), line.slice(10, line.size) + "\r\n")
            Some(rec)
        }
        else {
            fstream.close()
            fstream = null
            None
        }
    }
    
    def close(): Unit = {
        if (fstream != null) {
            fstream.close()
            fstream = null
        }
    }

    def removeFiles(): Unit = {
        assert(fileList != Nil)
        assert(fstream == null)

        log.info("removing files: " + fileList.toString)

        val deletedFiles = for {
            file <- fileList
            if (file.delete())
        } yield file

        assert(fileList.size == deletedFiles.size)

        fileList = List.empty
    }
}

class MultiFileWrite(private[this] val filePath: String) {
    val log = LoggerFactory.getLogger(getClass)

    private[this] var fileIdx, fileOff = 0
    private[this] var fileList: List[File] = List.empty

    private[this] var fstream: BufferedWriter = null

    def writeOneRecord(rec: Record): Unit = {
        assert(rec.key.size + rec.value.size == 100)

        if (fileIdx >= fileList.size) { // including fileList == List.empty
            assert(fstream == null)
            fileList = fileList :+ new File(filePath + "/" + MultiFileWrite.getFreshFileName())
            fstream = new BufferedWriter(new FileWriter(fileList(fileIdx)))
        }

        fstream.write(rec.key + rec.value)
        fileOff += 100 // 100 = record size
        if (fileOff >= 32 * 1000 * 1000) { // TODO: move this literal to config
            fstream.flush()
            assert(fileList(fileIdx).length().toInt == 32 * 1000 * 1000)

            fileOff -= fileList(fileIdx).length().toInt
            assert(fileOff == 0)
            fileIdx += 1

            fstream.close()
            fstream = null
            // file added later
        }
    }

    def close(): Unit = {
        if (fstream != null) {
            fstream.close()
            fstream = null
        }
    }

    def getFileList(): List[File] = fileList

    def renameFiles(fl: List[File]): Boolean = {
        assert(fileList.size == fl.size)

        val fl_zip = fileList.zip(fl)
        val res = fl_zip.foldLeft(true)((acc, p) => {
            acc && Try(p._1.renameTo(p._2)).getOrElse(false)
        })

        if (res) {
            fileList = fl
        }
        res
    }
}

object MultiFileWrite {
    var fileNameCounter = 0

    def getFreshFileName(): String = {
        val fileName = "merging." + fileNameCounter.toString
        fileNameCounter += 1
        fileName
    }
}