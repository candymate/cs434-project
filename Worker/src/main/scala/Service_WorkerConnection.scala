import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToWorkerChannel
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectResponse, Empty, connectWorkerServiceGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

class Service_WorkerConnection extends connectWorkerServiceGrpc.connectWorkerService {
    val log: Logger = LoggerFactory.getLogger(getClass)
    private val lock = new ReentrantLock()

    override def masterToWorkerConnectResponse(connectResponse: ConnectResponse): Future[Empty] = {
        assert(WORKER_STATE == CONNECTION_FINISH)

        log.info("Master to Worker response connect response received")
        lock.lock()
        try {
            WorkerToWorkerChannel.ipList = connectResponse.ipList.toArray
            WorkerToWorkerChannel.portList = connectResponse.portList.toArray
            WORKER_STATE = SAMPLING_START
            Future.successful {
                Empty()
            }
        } finally {
            lock.unlock()
        }
    }
}