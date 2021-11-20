import org.slf4j.LoggerFactory

import java.io.File
import java.io.RandomAccessFile

class Record(val key: String, val value: String) {
  override def toString: String = {
    "Key: " + key + " / Value: " + value
  }
}

class MultiFile(private[this] var fileList: List[File]) {
  val logger = LoggerFactory.getLogger(getClass)

  private[this] var fileReadIdx, fileReadOff = 0
  private[this] var fileWriteIdx, fileWriteOff = 0
  private[this] var readOnly: Boolean = false

  def merge(mf1: MultiFile, mf2: MultiFile): MultiFile = {
    mf1
  }

  def isEmpty(): Boolean = fileList.isEmpty

  def isSingleFile(): Boolean = {
    assert(fileList != Nil)
    fileList.size == 1
  }

  def seekRead(idx: Int, off: Int): (Int, Int) = {
    fileReadIdx = idx
    fileReadOff = off
    (fileReadIdx, fileReadOff)
  }

  def seekWrite(idx: Int, off: Int): (Int, Int) = {
    fileWriteIdx = idx
    fileWriteOff = off
    (fileWriteIdx, fileWriteOff)
  }

  def readOneRecord(): Record = {
    val stream = new RandomAccessFile(fileList(fileReadIdx), "r")
    stream.seek(fileReadOff)

    val line = stream.readLine()
    fileReadOff += line.size + 2 // gensort uses \r\n
    if (fileReadOff >= stream.length()) {
      fileReadOff -= stream.length().toInt
      fileReadIdx += 1
    }
    
    val rec = new Record(line.slice(0,10), line.slice(10,line.size))
    stream.close()
    rec
  }

  def writeOneRecord(rec: Record): Unit = {

  }

  def removeFiles(): Unit = {
    val deletedFiles = for {
      file <- fileList
      if (file.delete())
    } yield file

    assert(fileList.size == deletedFiles.size)

    fileList = List()
  }
}