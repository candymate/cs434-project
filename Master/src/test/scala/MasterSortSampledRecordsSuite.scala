import org.scalatest.funsuite.AnyFunSuite

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MasterSortSampledRecordsSuite extends AnyFunSuite {
    test("Test sorting function") {
        val stringList = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m").reverse
        Master.numOfRequiredConnections = 3

        MasterSortSampledRecords.pivotFromSampledRecords(stringList)

        assert(MasterSortSampledRecords.pivotList(0).equals("a"))
        assert(MasterSortSampledRecords.pivotList(1).equals("e"))

        assert(MasterSortSampledRecords.pivotIndex(0) == 0)
        assert(MasterSortSampledRecords.pivotIndex(1) == 4)
        assert(MasterSortSampledRecords.pivotIndex(2) == 8)
    }
}
