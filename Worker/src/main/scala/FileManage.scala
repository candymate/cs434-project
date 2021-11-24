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

  def length(): Int = {
    assert(fileList != Nil)
    fileList.foldLeft(0)((acc, f) => acc + f.length.toInt)
  }

  def seekRead(idx: Int, off: Int): (Int, Int) = {
    assert(fileList != Nil)
    assert(idx < fileList.size && off < fileList(idx).length.toInt)

    fileIdx = idx
    fileOff = off
    (fileIdx, fileOff)
  }

  def readOneRecord(): Option[Record] = {
    assert(fileList != Nil)
    
    if (fileIdx >= fileList.size) {
      return None
    }

    val stream = new RandomAccessFile(fileList(fileIdx), "r")
    stream.seek(fileOff)

    // TODO: IOException Handling

    val line = stream.readLine()
    if (line.size == 100 - 2) {
      fileOff += line.size + 2 // gensort uses \r\n
      if (fileOff >= stream.length()) {
        fileOff -= stream.length().toInt
        fileIdx += 1
      }
      val rec = new Record(line.slice(0,10), line.slice(10,line.size)+"\r\n")
      stream.close()
      Some(rec)
    }
    else {
      stream.close()
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

  def seekWrite(idx: Int, off: Int): (Int, Int) = {
    assert(idx < fileList.size && off < fileList(idx).length.toInt)

    fileIdx = idx
    fileOff = off
    (fileIdx, fileOff)
  }

  def writeOneRecord(rec: Record): Unit = {
    assert(rec.key.size + rec.value.size == 100)

    if (fileIdx >= fileList.size) {
      fileList = fileList :+ new File(filePath + "/" + MultiFileWrite.getFreshFileName())
    }

    val stream = new RandomAccessFile(fileList(fileIdx), "rw")
    stream.seek(fileOff)
    stream.writeBytes(rec.key + rec.value)
    fileOff += 100 // 100 = record size
    if (fileOff >= 32*1000*1000) { // TODO: move this literal to config
      assert(stream.length().toInt == 32*1000*1000)

      fileOff -= stream.length().toInt
      fileIdx += 1
      // file added later
    }

    stream.close()
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