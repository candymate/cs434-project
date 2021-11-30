import Worker.WORKER_STATE
import WorkerState._
import protobuf.connect.{Empty, mergeStartToMergeFinishWorkerGrpc, shuffleStartToShuffleFinishWorkerGrpc}

import scala.concurrent.Future

class Service_WorkerShuffle extends mergeStartToMergeFinishWorkerGrpc.mergeStartToMergeFinishWorker {
    override def startMerging(request: Empty): Future[Empty] = {
        assert(WORKER_STATE == MERGE_START)
        WORKER_STATE = MERGE_FINISH

        Future.successful {
            Empty()
        }
    }
}
