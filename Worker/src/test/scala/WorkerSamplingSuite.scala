import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class WorkerSamplingSuite extends AnyFunSuite {

    test("Sampling Function (From file) Unit Test") {
        val testFile = new File("Worker//data//partition1")

        val stringList = WorkerSampling.sampleFromFile(testFile)

        assertResult(3000) (stringList.size)
        assert("AsfAGHM5om".equals(stringList(0)))
        assert("Ga]QGzP2q)".equals(stringList(2999)))
    }
}
