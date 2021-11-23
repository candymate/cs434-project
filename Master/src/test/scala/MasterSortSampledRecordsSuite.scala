import org.scalatest.funsuite.AnyFunSuite

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MasterSortSampledRecordsSuite extends AnyFunSuite {
    test("Test sorting function") {
        val stringList = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m").reverse
        val requiredConnections = 3

        val sampledSortedRecords = new MasterSortSampledRecords(stringList, requiredConnections)

        val sortedStringList = sampledSortedRecords.sortedSampledRecords
        val pivotIndex = sampledSortedRecords.pivotIndex

        assert(sortedStringList(0).equals("a"))
        assert(sortedStringList(12).equals("m"))

        assert(pivotIndex(0) == 0)
        assert(pivotIndex(1) == 4)
        assert(pivotIndex(2) == 8)
    }
}
