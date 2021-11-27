import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import config.ClientInfo
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, SamplingResponse, SortingResponse, connectMasterServiceGrpc, sampleMasterServiceGrpc, sortMasterServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterSort {
    var numberOfClientsThatFinishedSort: Int = 0

    class Service_MasterSort extends sortMasterServiceGrpc.sortMasterService {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def workerToMasterSortResponse(response: SortingResponse): Future[Empty] = synchronized {
            assert(MASTER_STATE == SORT_PARTITION_START)

            lock.lock()
            try {
                numberOfClientsThatFinishedSort += 1
                if (numberOfClientsThatFinishedSort == Master.numOfRequiredConnections) {
                    MASTER_STATE = SORT_PARTITION_FINISH
                    MASTER_STATE = SHUFFLE_START
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}