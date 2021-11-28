import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import scala.io.Source.fromFile

@RunWith(classOf[JUnitRunner])
class WorkerSortAndPartitionSuite extends AnyFunSuite {

    test("sortAndPartitionFromInputFile test") {
        val testDirectory = new File("Worker//data//sortA1")
        val testOutputDirectory = new File("Worker//output//output2")

        val pivotList = List("AsfAGHM5om", "eSzU!X,[%/")

        WorkerSortAndPartition.
            sortAndPartitionFromInputFile(testDirectory, testOutputDirectory, pivotList)

        val file1Size = fromFile("Worker//output//output2//unshuffled.0.0").getLines().size

        assert(142 == file1Size)
    }
}
