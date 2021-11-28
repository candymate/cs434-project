import Master.MASTER_STATE
import MasterState._
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{Empty, sortPartitionStartToSortPartitionFinishMasterGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterSort {
    var numberOfClientsThatFinishedSort: Int = 0

    class Service_MasterSort extends sortPartitionStartToSortPartitionFinishMasterGrpc.sortPartitionStartToSortPartitionFinishMaster {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def finishedSorting(response: Empty): Future[Empty] = synchronized {
            assert(MASTER_STATE == SORT_PARTITION_START)

            lock.lock()
            try {
                numberOfClientsThatFinishedSort += 1
                if (numberOfClientsThatFinishedSort == Master.numOfRequiredConnections) {
                    MASTER_STATE = SORT_PARTITION_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}