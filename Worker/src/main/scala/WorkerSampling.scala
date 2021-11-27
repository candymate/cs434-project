import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{Empty, SamplingRequest, SamplingResponse, sampleMasterServiceGrpc, sampleWorkerServiceGrpc}

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future
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

        try {
            fromFile(inputFilePath.getPath).getLines.take(numberOfFetchedRecords).toList.map(_.slice(0, 10))
        } finally {
            fromFileBuffer.close()
        }
    }

    def sendSampledDataToMaster() = {
        val sampledData = sampleFromFile(WorkerArgumentHandler.inputFileArray(0))
        // WorkerToMasterChannel.openWorkerToMasterChannel()

        val blockingStub = sampleMasterServiceGrpc.blockingStub(WorkerToMasterChannel.channel)

        try {
            val request = blockingStub.workerToMasterSampleResponse(new SamplingResponse(
                sampledData
            ))
            WORKER_STATE = SORT_PARTITION_START
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
