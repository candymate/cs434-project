import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{Empty, sortPartitionStartToSortPartitionFinishWorkerGrpc}

class Request_MasterSort(channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastSortStart() = {
        def broadcastSortMessage(x: ManagedChannel) = {
            val blockingStub = sortPartitionStartToSortPartitionFinishWorkerGrpc.blockingStub(x)

            assert(MasterSortSampledRecords.pivotList != null)

            blockingStub.startSorting(new Empty())
        }

        if (channelArray.size == 0) {
            assert(MASTER_STATE == SORT_PARTITION_START)
            // MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastSortMessage)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastSortMessage(x))
        }
    }
}
