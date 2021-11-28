import Master.MASTER_STATE
import MasterState._
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{Empty, shuffleStartToShuffleFinishMasterGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterShuffle {
    var numberOfClientsThatFinishedShuffle: Int = 0

    class Service_MasterShuffle extends shuffleStartToShuffleFinishMasterGrpc.shuffleStartToShuffleFinishMaster {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def finishedShuffling(response: Empty): Future[Empty] = synchronized {
            assert(MASTER_STATE == SHUFFLE_START)

            lock.lock()
            try {
                numberOfClientsThatFinishedShuffle += 1
                if (numberOfClientsThatFinishedShuffle == Master.numOfRequiredConnections) {
                    MASTER_STATE = SHUFFLE_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}