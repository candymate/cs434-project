import WorkerState._
import channel.WorkerToMasterChannel
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Worker {
    @volatile var WORKER_STATE: WorkerState = CONNECTION_START

    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        log.info("Handling argument")
        WorkerArgumentHandler.handleArgument(args)

        log.info("Connection phase start")
        val workerConnection = new WorkerConnection(null)
        workerConnection.initiateConnection()
        WORKER_STATE = CONNECTION_FINISH
        log.info("Connection phase successfully finished")

        log.info("Worker Server start for communication")
        val workerServer = new WorkerServer(WorkerArgumentHandler.inputFileArray, WorkerArgumentHandler.outputFile, ExecutionContext.global)
        log.info("Worker Server start completed for communication")

        // sampling phase (server required in worker)

        // shuffling (depending on implementation)

        // merging phase (server required in worker)

        // checking (server required in worker)
    }
}
