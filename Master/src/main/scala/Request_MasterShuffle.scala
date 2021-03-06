import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{Empty, shuffleStartToShuffleFinishWorkerGrpc}

class Request_MasterShuffle(channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastShuffleStart() = {
        def broadcastShuffleMessage(x: ManagedChannel) = {
            val blockingStub = shuffleStartToShuffleFinishWorkerGrpc.blockingStub(x)

            // assert(MasterShuffleSampledRecords.pivotList != null)
            // TODO: check invariants here

            blockingStub.startShuffling(new Empty())
        }

        if (channelArray.size == 0) {
            assert(MASTER_STATE == SHUFFLE_START)
            // MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastShuffleMessage)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastShuffleMessage(x))
        }
    }
}
