import Worker.WORKER_STATE
import WorkerState.{SAMPLING_SAMPLE, SAMPLING_START}
import protobuf.connect.{Empty, samplingStartToSamplingSampleWorkerGrpc}

import scala.concurrent.Future

class Service_WorkerSampleFirst extends samplingStartToSamplingSampleWorkerGrpc.samplingStartToSamplingSampleWorker {
    override def startSampling(request: Empty): Future[Empty] = {
        assert(WORKER_STATE == SAMPLING_START)
        WORKER_STATE = SAMPLING_SAMPLE

        Future.successful {
            Empty()
        }
    }
}
