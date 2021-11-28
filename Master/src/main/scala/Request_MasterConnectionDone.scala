import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import io.grpc.{Channel, ManagedChannel}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectResponse, connectionStartToConnectionFinishWorkerGrpc}

class Request_MasterConnectionDone(channelArrayParam: Array[ManagedChannel]) {
    val log: Logger = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastConnectionDone() = {
        def broadcastConnectionResponse(x: Channel) = {
            val blockingStub = connectionStartToConnectionFinishWorkerGrpc.blockingStub(x)
            blockingStub.broadCastClientInfo(new ConnectResponse(
                MasterToWorkerChannel.ipList, MasterToWorkerChannel.portList))
        }

        if (channelArray.size == 0) {
            assert(MASTER_STATE == CONNECTION_FINISH)
            MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastConnectionResponse)
            // MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach (x => broadcastConnectionResponse(x))
        }
    }
}
