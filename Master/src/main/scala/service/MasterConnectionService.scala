import Master.MASTER_STATE
import MasterState._
import scala.concurrent.Future
import java.util.concurrent.locks.ReentrantLock
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, ConnectResponse, Empty, connectMasterServiceGrpc, connectWorkerServiceGrpc}
import java.net.InetAddress
import config.ClientInfo
import channel.MasterToWorkerChannel

class MasterConnectionService extends connectMasterServiceGrpc.connectMasterService {
    val log: Logger = LoggerFactory.getLogger(getClass)
    private val lock = new ReentrantLock()

    override def workerToMasterConnect(request: ConnectRequest): Future[Empty] = synchronized {
        log.info("connection established with " + request.ip + ":" + request.port)

        lock.lock()
        try {
            Master.clientInfoMap.put(Master.clientInfoMap.size, new ClientInfo(request.ip, request.port))
            if (Master.numOfRequiredConnections == Master.clientInfoMap.size) {
                log.info(s"Master successfully connected to ${Master.numOfRequiredConnections} client(s)")

                MasterToWorkerChannel.configureClientIpAndPort(request.ip, request.port)

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