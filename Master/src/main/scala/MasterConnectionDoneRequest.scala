import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.ManagedChannel
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectResponse, connectionStartToConnectionFinishWorkerGrpc}

class MasterConnectionDoneRequest(channelArrayParam: Array[ManagedChannel]) {
    val log: Logger = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastConnectionDone() = {
        assert(MASTER_STATE == CONNECTION_FINISH)

        def broadcastConnectionResponse(x: ManagedChannel) = {
            val blockingStub = connectionStartToConnectionFinishWorkerGrpc.blockingStub(x)
            blockingStub.broadCastClientInfo(new ConnectResponse(
                MasterToWorkerChannel.ipList, MasterToWorkerChannel.portList))
        }

        if (channelArray == null) {
            MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastConnectionResponse)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastConnectionResponse(x))
        }
    }
}