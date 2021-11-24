import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import scala.io.Source.fromFile

@RunWith(classOf[JUnitRunner])
class WorkerSortAndPartitionSuite extends AnyFunSuite {

    test("sortAndPartitionFromInputFile test") {
        val testDirectory = new File("Worker//data//sortA1")
        val testOutputDirectory = new File("Worker//data")

        val pivotList = List("AsfAGHM5om")

        WorkerSortAndPartition.
        sortAndPartitionFromInputFile(testDirectory, testOutputDirectory, pivotList)

        val file1Size = fromFile("Worker//data//unshuffled.0.0").getLines().size
        val file2Size = fromFile("Worker//data//unshuffled.1.0").getLines().size

        assert(68 == file1Size)
        assert(132 == file2Size)
    }
}
