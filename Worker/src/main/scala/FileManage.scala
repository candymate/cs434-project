import org.slf4j.LoggerFactory

import java.io._

class Record(val key: String, val value: String) {
  override def toString: String = {
    "Key: " + key + " / Value: " + value
  }
}

class MultiFileRead(private[this] val fileList: List[File]) {
  val logger = LoggerFactory.getLogger(getClass)

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

  def readOneRecord(): Record = {
    assert(fileList != Nil)

    val stream = new RandomAccessFile(fileList(fileIdx), "r")
    stream.seek(fileOff)

    val line = stream.readLine()
    fileOff += line.size + 2 // gensort uses \r\n
    if (fileOff >= stream.length()) {
      fileOff -= stream.length().toInt
      fileIdx += 1
    }
    
    val rec = new Record(line.slice(0,10), line.slice(10,line.size))
    stream.close()
    rec
  }

  def removeFiles(): Unit = {
    assert(fileList != Nil)

    val deletedFiles = for {
      file <- fileList
      if (file.delete())
    } yield file

    assert(fileList.size == deletedFiles.size)
  }
}

class MultiFileWrite(private[this] val filePath: String) {
  val logger = LoggerFactory.getLogger(getClass)

  private[this] var fileIdx, fileOff = 0
  private[this] var fileList: List[File] = List.empty

  def seekWrite(idx: Int, off: Int): (Int, Int) = {
    assert(idx < fileList.size && off < fileList(idx).length.toInt)

    fileIdx = idx
    fileOff = off
    (fileIdx, fileOff)
  }

  def writeOneRecord(rec: Record): Unit = {
    if (fileList.isEmpty) {
      assert(fileIdx == 0 && fileOff == 0)

      fileList = List(new File(filePath + "/" + MultiFileWrite.getFreshFileName()))
    }

    val stream = new RandomAccessFile(fileList(fileIdx), "rw")
    stream.seek(fileOff)
    stream.writeBytes(rec.key + rec.value)
    fileOff += 100 // 100 = record size
    if (fileOff >= 32*1000*1000) { // TODO: move this literal to config
      assert(stream.length().toInt == 32*1000*1000)
      
      fileOff -= stream.length().toInt
      fileIdx += 1
      fileList = fileList :+ new File(filePath + "/" + MultiFileWrite.getFreshFileName())
    }

    stream.close()
  }

  def getFileList(): List[File] = fileList
}

object MultiFileWrite {
  var fileNameCounter = 0

  def getFreshFileName(): String = {
    val fileName = "merging." + fileNameCounter.toString
    fileNameCounter += 1
    fileName
  }
}