import Master.MASTER_STATE
import MasterState.SAMPLING_PIVOT
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{PivotResult, samplingSampleToSamplingFinishWorkerGrpc}

class Request_MasterSampleSecond(channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastPivots() = {
        def broadcastPivotMessages(x: ManagedChannel) = {
            val blockingStub = samplingSampleToSamplingFinishWorkerGrpc.blockingStub(x)
            blockingStub.pivotResult(new PivotResult(MasterSortSampledRecords.pivotList))
        }

        if (channelArray.size == 0) {
            assert(MASTER_STATE == SAMPLING_PIVOT)
            // MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastPivotMessages)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastPivotMessages(x))
        }
    }
}
