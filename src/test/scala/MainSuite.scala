import config.{ClientInfo, MasterConfig}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source.fromFile

@RunWith(classOf[JUnitRunner])
class MainSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Connection test") {
        val numberOfConnection = 5
        val clientConnection: List[Future[Unit]] = Nil

        val openServer = Future {
            val testMaster = new MasterConnection(numberOfConnection, executorContext)

            assert(testMaster != null)

            Thread.sleep(500)

            assert(testMaster.server.isTerminated)
        }

        for(
            i <- 0 until numberOfConnection
        ) yield {
            clientConnection :+ Future {
                val testClient = new WorkerConnection(new MasterConfig("127.0.0.1", 9000), null)

                testClient.connect()
            }
        }

        Await.result(openServer, Duration.Inf)
        clientConnection foreach(x =>
            Await.result(x, Duration.Inf)
        )
    }

    test("Sampling phase test") {
        val openServer = Future {
            val testFile = new File("Worker//data//partition1")
            val openSamplingServer = new WorkerServer(Array(testFile), null, executorContext)

            Thread.sleep(500)
        }

        val managedChannelBuilder = ManagedChannelBuilder.forAddress("localhost", 8000)
        managedChannelBuilder.usePlaintext()
        val channel = managedChannelBuilder.build()

        val clientInfoMap = mutable.Map[Int, ClientInfo]()

        val masterClient = new MasterSampleSortRequest(clientInfoMap, Array(channel), null)
        masterClient.sendSampleRequestToEveryClient()

        assertResult(3000) (masterClient.sampledData.size)

        assert("AsfAGHM5om".equals(masterClient.sampledData(0)))
        assert("Ga]QGzP2q)".equals(masterClient.sampledData(2999)))
    }

    test("sort and partition test") {
        val openServer = Future {
            val testInputFile1 = new File("Worker//data//partition1")
            val testInputFile2 = new File("Worker//data//sortA1")
            val testInputFile3 = new File("Worker//data//sortA2")
            val testInputFile4 = new File("Worker//data//sortA3")
            val testOutputDirectory = new File("Worker//output")
            val openSortingServer = new WorkerServer(Array(testInputFile1, testInputFile2
            , testInputFile3, testInputFile4), testOutputDirectory, executorContext)

            Thread.sleep(500)
        }

        val managedChannelBuilder = ManagedChannelBuilder.forAddress("localhost", 8000)
        managedChannelBuilder.usePlaintext()
        val channel = managedChannelBuilder.build()

        val clientInfoMap = mutable.Map[Int, ClientInfo]()

        Thread.sleep(500)

        val requestServer = new MasterSampleSortRequest(clientInfoMap, Array(channel),
            List("8mdOTG)j6O", "AsAGHM5om", "B]az'~,CNd"))
        requestServer.sendSortRequestToEveryClient()

        val outputFile00 = fromFile("Worker//output//unshuffled.0.0").getLines().size
        val outputFile01 = fromFile("Worker//output//unshuffled.0.1").getLines().size
        val outputFile02 = fromFile("Worker//output//unshuffled.0.2").getLines().size
        val outputFile03 = fromFile("Worker//output//unshuffled.0.3").getLines().size

        val outputFile10 = fromFile("Worker//output//unshuffled.1.0").getLines().size
        val outputFile11 = fromFile("Worker//output//unshuffled.1.1").getLines().size
        val outputFile12 = fromFile("Worker//output//unshuffled.1.2").getLines().size
        val outputFile13 = fromFile("Worker//output//unshuffled.1.3").getLines().size

        val outputFile20 = fromFile("Worker//output//unshuffled.2.0").getLines().size
        val outputFile21 = fromFile("Worker//output//unshuffled.2.1").getLines().size
        val outputFile22 = fromFile("Worker//output//unshuffled.2.2").getLines().size
        val outputFile23 = fromFile("Worker//output//unshuffled.2.3").getLines().size

        assert(1073 == outputFile00)
        assert(24 == outputFile10)
        assert(1903 == outputFile20)

        assert(outputFile01 == 68)
        assert(outputFile11 == 1)
        assert(outputFile21 == 131)

        assert(outputFile02 == 68)
        assert(outputFile12 == 1)
        assert(outputFile22 == 131)

        assert(outputFile03 == 68)
        assert(outputFile13 == 1)
        assert(outputFile23 == 131)
    }
}
