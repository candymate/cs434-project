import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import scala.io.Source.fromFile

@RunWith(classOf[JUnitRunner])
class WorkerSortAndPartitionSuite extends AnyFunSuite {

    test("sortAndPartitionFromInputFile test") {
        val testDirectory = new File("Worker//data//sortA1")
        val testOutputDirectory = new File("Worker//output")

        val pivotList = List("AsfAGHM5om")

        WorkerSortAndPartition.
        sortAndPartitionFromInputFile(testDirectory, testOutputDirectory, pivotList)

        val file1Size = fromFile("Worker//output//unshuffled.0.0").getLines().size
        val file2Size = fromFile("Worker//output//unshuffled.1.0").getLines().size

        assert(68 == file1Size)
        assert(132 == file2Size)
    }

    test("sortAndPartitionFromInputFileList test") {
        val testDirectory = new File("Worker//data//sortA1")
        val testDirectory2 = new File("Worker//data//sortA2")
        val testDirectory3 = new File("Worker//data//sortA3")
        val testOutputDirectory = new File("Worker//output")

        val pivotList = List("AsfAGHM5om")

        WorkerSortAndPartition.
            sortAndPartitionFromInputFileList(
                Array(testDirectory, testDirectory2, testDirectory3), testOutputDirectory, pivotList)

        val file1Size1 = fromFile("Worker//output//unshuffled.0.0").getLines().size
        val file2Size1 = fromFile("Worker//output//unshuffled.1.0").getLines().size

        assert(68 == file1Size1)
        assert(132 == file2Size1)

        val file1Size2 = fromFile("Worker//output//unshuffled.0.1").getLines().size
        val file2Size2 = fromFile("Worker//output//unshuffled.1.1").getLines().size

        assert(68 == file1Size2)
        assert(132 == file2Size2)

        val file1Size3 = fromFile("Worker//output//unshuffled.0.2").getLines().size
        val file2Size3 = fromFile("Worker//output//unshuffled.1.2").getLines().size

        assert(68 == file1Size3)
        assert(132 == file2Size3)
    }
}
