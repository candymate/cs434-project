import WorkerState.SORT_PARTITION_FINISH
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import scala.io.Source.fromFile

@RunWith(classOf[JUnitRunner])
class WorkerSortSuite extends AnyFunSuite {
    test("sortAndPartitionFromInputFileList test") {
        val testDirectory = new File("Worker//data//sortA1")
        val testDirectory2 = new File("Worker//data//sortA2")
        val testDirectory3 = new File("Worker//data//sortA3")
        val testOutputDirectory = new File("Worker//output")

        val pivotList = List("AsfAGHM5om")

        WorkerSortAndPartition.pivotList = pivotList
        Worker.WORKER_STATE = SORT_PARTITION_FINISH

        WorkerSortAndPartition.
            sortAndPartitionFromInputFileList(
                Array(testDirectory, testDirectory2, testDirectory3), testOutputDirectory)

        val file1Size1 = fromFile("Worker//output//unshuffled.0.0").getLines().size
        val file2Size1 = fromFile("Worker//output//unshuffled.0.1").getLines().size
        val file3Size1 = fromFile("Worker//output//unshuffled.0.2").getLines().size

        assert(200 == file1Size1)
        assert(200 == file2Size1)
        assert(200 == file3Size1)
    }
}
