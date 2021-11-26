import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import config.{ClientInfo, MasterServerConfig}
import io.grpc.{ManagedChannel, Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, ConnectResponse, Empty, connectMasterServiceGrpc, connectWorkerServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.{ExecutionContext, Future}

class MasterConnectionDoneRequest(channelArrayParam: Array[ManagedChannel]) {
    val log: Logger = LoggerFactory.getLogger(getClass)
    var channelArray: Array[ManagedChannel] = channelArrayParam

    def broadcastConnectionDone() = {
        assert (MASTER_STATE == CONNECTION_FINISH)

        def broadcastConnectionResponse(x: ManagedChannel) = {
            val blockingStub = connectWorkerServiceGrpc.blockingStub(x)
            blockingStub.masterToWorkerConnectResponse(new ConnectResponse(
                MasterToWorkerChannel.ipList, MasterToWorkerChannel.portList))
        }

        if (channelArray == null) {
            MasterToWorkerChannel.openMasterToWorkerChannelArray()
            MasterToWorkerChannel.sendMessageToEveryClient(broadcastConnectionResponse)
            MasterToWorkerChannel.closeMasterToWorkerChannelArray()
        } else {
            channelArray foreach(x => broadcastConnectionResponse(x))
        }

        MASTER_STATE = SAMPLING_START
    }
}
