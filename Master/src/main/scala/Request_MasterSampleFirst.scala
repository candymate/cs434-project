import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{Empty, samplingStartToSamplingSampleWorkerGrpc}

class Request_MasterSampleFirst(channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastSampleStart() = {
        assert(MASTER_STATE == SAMPLING_START)

        def broadcastSampleMessage(x: ManagedChannel) = {
            val blockingStub = samplingStartToSamplingSampleWorkerGrpc.blockingStub(x)
            blockingStub.startSampling(Empty())
        }

        if (channelArray.size == 0) {
            // MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastSampleMessage)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastSampleMessage(x))
        }
    }
}
