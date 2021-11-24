import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.Source.fromFile

object WorkerSortAndPartition {
    var fileNamePartition = 0
    val numberOfRecords = 1000000

    // all file list
    def sortAndPartitionFromInputFileList(inputPathFileList: Array[File],
                                          outputPathFile: File,
                                          pivotMap: List[String]) = {
        inputPathFileList foreach {
            sortAndPartitionFromInputFile(_, outputPathFile, pivotMap)
        }
    }

    // single file
    def sortAndPartitionFromInputFile(inputPathFile: File, outputPathFile: File,
                                      pivotMap: List[String]) = {
        val sortedDataFromFile = fromFile(inputPathFile.getPath).getLines().toList
            .sortWith(_.slice(0, 10) < _.slice(0, 10))
        makeSortedPartition(sortedDataFromFile, pivotMap, outputPathFile.getPath, 0)

        fileNamePartition += 1
    }

    @tailrec
    def makeSortedPartition(sortedDataFromFile: List[String], pivotMap: List[String],
                           outputPathFile: String, targetMachine: Int): Any = {
        pivotMap match {
            case head::tail => {
                val (firstList, restList) = sortedDataFromFile.partition(_.slice(0, 10) < head)
                val newPath = outputPathFile + "/" + generateName(targetMachine)
                val bufferedWriter = new BufferedWriter(new FileWriter(new File(newPath)))
                sortedDataFromFile.foreach(
                    bufferedWriter.write(_)
                )
                bufferedWriter.close()
                makeSortedPartition(restList, tail, outputPathFile, targetMachine + 1)
            }
            case (e: Exception) => {
                sys.exit(1)
            }
        }
    }

    private def generateName(targetWorker: Int): String = {
        val fileName = "unshuffled." + fileNamePartition + "." + targetWorker
        fileName
    }
}
