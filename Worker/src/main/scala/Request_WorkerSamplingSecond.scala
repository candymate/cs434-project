import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{Empty, SamplingResponse, samplingPivotToSamplingFinishMasterGrpc, samplingStartToSamplingPivotMasterGrpc}

import java.io.File
import scala.io.BufferedSource
import scala.io.Source.fromFile

object Request_WorkerSamplingSecond {
    def sendSampledDataToMaster() = {
        assert(WORKER_STATE == SAMPLING_FINISH)

        val blockingStub = samplingPivotToSamplingFinishMasterGrpc.blockingStub(WorkerToMasterChannel.channel)

        try {
            val request = blockingStub.samplePartitionFinished(new Empty())
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
