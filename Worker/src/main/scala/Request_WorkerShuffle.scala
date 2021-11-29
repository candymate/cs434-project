import Worker.WORKER_STATE
import WorkerState.SHUFFLE_FINISH
import channel.WorkerToMasterChannel
import io.grpc.StatusRuntimeException
import protobuf.connect.{Empty, shuffleStartToShuffleFinishMasterGrpc}

object Request_WorkerShuffle {
    def sendShuffleFinished(): Unit = {
        assert(WORKER_STATE == SHUFFLE_FINISH)

        val blockingStub = shuffleStartToShuffleFinishMasterGrpc.blockingStub(
            WorkerToMasterChannel.channel
        )

        try {
            val request = blockingStub.finishedShuffling(new Empty())
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
