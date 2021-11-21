import java.io.File
import scala.io.{BufferedSource, Source}
import scala.io.Source.{fromBytes, fromFile}

object WorkerSampling {
    val numberOfRecords = 100000

    def sampleFromFile(inputFilePath: File): List[String] = {
        val fromFileBuffer: BufferedSource = fromFile(inputFilePath.getPath)
        val numberOfLines = fromFileBuffer.getLines().size
        val numberOfFetchedRecords = if (numberOfRecords < numberOfLines) {
            numberOfRecords
        } else {
            numberOfLines
        }

        val t = 1

        try {
            fromFile(inputFilePath.getPath).getLines.take(numberOfFetchedRecords).toList.map(_.slice(0, 10))
        } finally {
            fromFileBuffer.close()
        }
    }
}
