import Worker.WORKER_STATE
import WorkerState.{MERGE_FINISH, SHUFFLE_FINISH}
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{Empty, mergeStartToMergeFinishMasterGrpc, shuffleStartToShuffleFinishMasterGrpc}

object Request_WorkerMerge {
    def sendMergeFinished(): Unit = {
        assert(WORKER_STATE == MERGE_FINISH)

        val blockingStub = mergeStartToMergeFinishMasterGrpc.blockingStub(
            WorkerToMasterChannel.channel
        )

        try {
            val request = blockingStub.finishedMerging(new Empty())
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
