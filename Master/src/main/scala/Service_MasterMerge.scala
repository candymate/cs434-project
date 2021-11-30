import Master.MASTER_STATE
import MasterState._
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{Empty, mergeStartToMergeFinishMasterGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterMerge {
    var numberOfClientsThatFinishedMerge: Int = 0

    class Service_MasterShuffle extends mergeStartToMergeFinishMasterGrpc.mergeStartToMergeFinishMaster {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def finishedMerging(response: Empty): Future[Empty] = synchronized {
            assert(MASTER_STATE == MERGE_START)

            lock.lock()
            try {
                numberOfClientsThatFinishedMerge += 1
                if (numberOfClientsThatFinishedMerge == Master.numOfRequiredConnections) {
                    MASTER_STATE = MERGE_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}