import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory
import protobuf.connect.{SamplingRequest, sampleWorkerServiceGrpc}

class MasterSampleRequest {
    val log = LoggerFactory.getLogger(getClass)

    def sendSampleRequestToEveryClient() = {
        assert(MASTER_STATE == SAMPLING_START)

        def broadcastSampleMessage(x: ManagedChannel) = {
            val blockingStub = sampleWorkerServiceGrpc.blockingStub(x)
            blockingStub.masterToWorkerSampleRequest(new SamplingRequest())
        }

        MasterToWorkerChannel.openMasterToWorkerChannelArray()
        MasterToWorkerChannel.sendMessageToEveryClient(broadcastSampleMessage)
        MasterToWorkerChannel.closeMasterToWorkerChannelArray()

        MASTER_STATE = SAMPLING_FINISH
    }
}
