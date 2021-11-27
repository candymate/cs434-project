import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{SamplingRequest, SortingRequest, sortWorkerServiceGrpc}

class Request_MasterSort(channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastSortStart() = {
        assert(MASTER_STATE == SORT_PARTITION_START)

        def broadcastSortMessage(x: ManagedChannel) = {
            val blockingStub = sortWorkerServiceGrpc.blockingStub(x)

            assert(MasterSortSampledRecords.pivotList != null)

            blockingStub.masterToWorkerSortRequest(new SortingRequest(MasterSortSampledRecords.pivotList))
        }

        if (channelArray == null) {
            // MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastSortMessage)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach(x => broadcastSortMessage(x))
        }
    }
}
