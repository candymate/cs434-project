import Worker.WORKER_STATE
import WorkerState._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.Source.fromFile

object WorkerSortAndPartition {
    var fileNamePartition = 0
    val numberOfRecords = 1000000
    var pivotList: List[String] = Nil
    val log: Logger = LoggerFactory.getLogger(getClass)

    // all file list
    def sortAndPartitionFromInputFileList(inputPathFileList: Array[File],
                                          outputPathFile: File) = {
        assert(WORKER_STATE == SORT_PARTITION_FINISH)
        assert(pivotList.size != 0)

        inputPathFileList foreach {
            x => sortAndPartitionFromInputFile(x, outputPathFile, pivotList)
        }
    }

    def priority(record: String) = record.slice(0, 10)

    // single file
    def sortAndPartitionFromInputFile(inputPathFile: File, outputPathFile: File,
                                      pivotMap: => List[String]) = {
        val bufferedSource = fromFile(inputPathFile.getPath)
        val sortedDataFromFile = bufferedSource.getLines().toList.sortWith(_.slice(0, 10) < _.slice(0, 10))
        makeSortedPartition(sortedDataFromFile, pivotMap, outputPathFile.getPath, 0)

        fileNamePartition += 1
        bufferedSource.close()
    }

    @tailrec
    def makeSortedPartition(sortedDataFromFile: => List[String], pivotMap: => List[String],
                            outputPathFile: String, targetMachine: Int): Any = {
        pivotMap match {
            case head :: tail => {
                if (tail.size != 0) {
                    val (firstList, restList) = sortedDataFromFile.partition(_.slice(0, 10) < tail.head)
                    writeToFile(outputPathFile, firstList, targetMachine)
                    makeSortedPartition(restList, tail, outputPathFile, targetMachine + 1)
                } else {
                    writeToFile(outputPathFile, sortedDataFromFile, targetMachine)
                }
            }
            case _ => {}
        }
    }

    def writeToFile(outputPathFile: String, data: => List[String], targetMachine: Int) = {
        val newPath = outputPathFile + "/" + generateName(targetMachine)
        log.info(s"Sorting Phase: Started Writing to ${newPath}")
        val bufferedWriter = new BufferedWriter(new FileWriter(new File(newPath)))
        data.foreach(
            x => bufferedWriter.write(x + "\r\n")
        )
        bufferedWriter.close()
        log.info(s"Sorting Phase: Finished Writing to ${newPath}")
    }

    private def generateName(targetWorker: Int): String = {
        val fileName = "unshuffled." + targetWorker + "." + fileNamePartition
        fileName
    }
}
