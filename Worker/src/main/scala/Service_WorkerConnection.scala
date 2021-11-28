import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToWorkerChannel
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectResponse, Empty, connectionStartToConnectionFinishWorkerGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

class Service_WorkerConnection extends connectionStartToConnectionFinishWorkerGrpc.connectionStartToConnectionFinishWorker {
    val log: Logger = LoggerFactory.getLogger(getClass)
    private val lock = new ReentrantLock()

    override def broadCastClientInfo(connectResponse: ConnectResponse): Future[Empty] = {
        assert(WORKER_STATE == CONNECTION_START)

        log.info("Master to Worker response connect response received")
        lock.lock()
        try {
            WorkerToWorkerChannel.ipList = connectResponse.ipList.toArray
            WorkerToWorkerChannel.portList = connectResponse.portList.toArray
            WORKER_STATE = CONNECTION_FINISH
            Future.successful {
                Empty()
            }
        } finally {
            lock.unlock()
        }
    }
}