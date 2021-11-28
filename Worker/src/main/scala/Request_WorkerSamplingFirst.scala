import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{SamplingResponse, samplingStartToSamplingPivotMasterGrpc}

import java.io.File
import scala.io.BufferedSource
import scala.io.Source.fromFile

object Request_WorkerSamplingFirst {
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
        assert(WORKER_STATE == SAMPLING_SAMPLE)
        val sampledData = sampleFromFile(WorkerArgumentHandler.inputFileArray(0))
        // WorkerToMasterChannel.openWorkerToMasterChannel()

        val blockingStub = samplingStartToSamplingPivotMasterGrpc.blockingStub(WorkerToMasterChannel.channel)

        try {
            val request = blockingStub.samplingResult(new SamplingResponse(
                sampledData
            ))
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
