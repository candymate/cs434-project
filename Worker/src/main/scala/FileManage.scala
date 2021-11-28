import org.slf4j.LoggerFactory

import java.io._
import scala.util.Try

class Record(val key: String, val value: String) {
    override def toString: String = {
        "Key: " + key + " / Value: " + value
    }
}

class MultiFileRead(private[this] var fileList: List[File]) {
    val log = LoggerFactory.getLogger(getClass)

    private[this] var fileIdx, fileOff = 0

    private[this] var fstream: RandomAccessFile = null

    def length(): Int = {
        assert(fileList != Nil)
        fileList.foldLeft(0)((acc, f) => acc + f.length.toInt)
    }

    def seekRead(idx: Int, off: Int): (Int, Int) = {
        assert(fileList != Nil)
        assert(idx < fileList.size && off < fileList(idx).length.toInt)

        fileIdx = idx
        fileOff = off

        if (fstream != null) {
            fstream.close()
        }
        fstream = new RandomAccessFile(fileList(fileIdx), "r")
        fstream.seek(fileOff)

        (fileIdx, fileOff)
    }

    def readOneRecord(): Option[Record] = {
        assert(fileList != Nil)

        if (fileIdx >= fileList.size) {
            return None
        }

        if (fstream == null) {
            fstream = new RandomAccessFile(fileList(fileIdx), "r")
        }

        val line = fstream.readLine()
        if (line.size == 100 - 2) {
            fileOff += line.size + 2 // gensort uses \r\n
            if (fileOff >= fstream.length()) {
                fileOff -= fstream.length().toInt
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

    def removeFiles(): Unit = {
        assert(fileList != Nil)

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

    private[this] var fstream: RandomAccessFile = null

    def seekWrite(idx: Int, off: Int): (Int, Int) = {
        assert(idx < fileList.size && off < fileList(idx).length.toInt)

        fileIdx = idx
        fileOff = off

        if (fstream != null) {
            fstream.close()
            fstream = new RandomAccessFile(fileList(fileIdx), "rw")
            fstream.seek(fileOff)
        }

        (fileIdx, fileOff)
    }

    def writeOneRecord(rec: Record): Unit = {
        assert(rec.key.size + rec.value.size == 100)

        if (fileIdx >= fileList.size) { // including fileList == List.empty
            fileList = fileList :+ new File(filePath + "/" + MultiFileWrite.getFreshFileName())
            fstream = new RandomAccessFile(fileList(fileIdx), "rw")
        }

        fstream.writeBytes(rec.key + rec.value)
        fileOff += 100 // 100 = record size
        if (fileOff >= 32 * 1000 * 1000) { // TODO: move this literal to config
            assert(fstream.length().toInt == 32 * 1000 * 1000)

            fileOff -= fstream.length().toInt
            fileIdx += 1

            fstream.close()
            fstream = null
            // file added later
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