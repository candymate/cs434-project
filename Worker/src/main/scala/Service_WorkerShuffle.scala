import Worker.WORKER_STATE
import WorkerState.{SORT_PARTITION_FINISH, SORT_PARTITION_START}
import protobuf.connect.{Empty, sortPartitionStartToSortPartitionFinishWorkerGrpc}

import scala.concurrent.Future

class Service_WorkerSort extends sortPartitionStartToSortPartitionFinishWorkerGrpc.sortPartitionStartToSortPartitionFinishWorker {
    override def startSorting(request: Empty): Future[Empty] = {
        assert(WORKER_STATE == SORT_PARTITION_START)
        WORKER_STATE = SORT_PARTITION_FINISH

        Future.successful {
            Empty()
        }
    }
}
