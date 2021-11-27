import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import config.ClientInfo
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, connectMasterServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

class Service_MasterConnection extends connectMasterServiceGrpc.connectMasterService {
    val log: Logger = LoggerFactory.getLogger(getClass)
    private val lock = new ReentrantLock()

    override def workerToMasterConnect(request: ConnectRequest): Future[Empty] = synchronized {
        log.info("connection established with " + request.ip + ":" + request.port)

        lock.lock()
        try {
            Master.clientInfoMap.put(Master.clientInfoMap.size, new ClientInfo(request.ip, request.port))
            MasterToWorkerChannel.configureClientIpAndPort(request.ip, request.port)
            if (Master.numOfRequiredConnections == Master.clientInfoMap.size) {
                log.info(s"Master successfully connected to ${Master.numOfRequiredConnections} client(s)")

                println(s"${InetAddress.getLocalHost().getHostAddress()}:9000")
                Master.clientInfoMap foreach { case (_, v: ClientInfo) => print(s"${v.ip} ") }
                println()

                MASTER_STATE = CONNECTION_FINISH
            }
        } finally {
            lock.unlock()
        }

        Future.successful(Empty())
    }
}