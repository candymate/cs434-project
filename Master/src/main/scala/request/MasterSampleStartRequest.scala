import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{SamplingRequest, sampleWorkerServiceGrpc}

class MasterSampleStartRequest (channelArrayParam: Array[ManagedChannel]) {
    val log = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastSampleStart() = {
        assert(MASTER_STATE == SAMPLING_START)

        def broadcastSampleMessage(x: ManagedChannel) = {
            val blockingStub = sampleWorkerServiceGrpc.blockingStub(x)
            blockingStub.masterToWorkerSampleRequest(new SamplingRequest())
        }

        if (channelArray == null) {
            MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastSampleMessage)
            MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach(x => broadcastSampleMessage(x))
        }

        MASTER_STATE = SAMPLING_FINISH
    }
}
