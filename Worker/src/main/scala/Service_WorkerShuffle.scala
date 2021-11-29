import Worker.WORKER_STATE
import WorkerState.{SHUFFLE_SERVICE, SHUFFLE_START}
import protobuf.connect.{Empty, shuffleStartToShuffleFinishWorkerGrpc}

import scala.concurrent.Future

class Service_WorkerShuffle extends shuffleStartToShuffleFinishWorkerGrpc.shuffleStartToShuffleFinishWorker {
    override def startShuffling(request: Empty): Future[Empty] = {
        assert(WORKER_STATE == SHUFFLE_START)
        WORKER_STATE = SHUFFLE_SERVICE

        Future.successful {
            Empty()
        }
    }
}
