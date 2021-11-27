import Worker.WORKER_STATE
import WorkerState.SORT_PARTITION_FINISH
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{Empty, SamplingResponse, sortPartitionStartToSortPartitionFinishMasterGrpc}

object Request_WorkerSort {
    def sendSortFinished(): Unit = {
        assert(WORKER_STATE == SORT_PARTITION_FINISH)

        val blockingStub = sortPartitionStartToSortPartitionFinishMasterGrpc.blockingStub(
            WorkerToMasterChannel.channel
        )

        try {
            val request = blockingStub.finishedSorting(new Empty())
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
