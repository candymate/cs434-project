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

        log.info("Start worker side server")
        val workerServer = new WorkerServer(ExecutionContext.global)
        workerServer.start()
        log.info("Successfully turned on worker side server")

        log.info("Connection phase start")
        val workerConnection = new WorkerConnection(null)
        workerConnection.initiateConnection()

        while (WORKER_STATE == CONNECTION_FINISH) {Thread.sleep(500)}
        log.info("Connection phase successfully finished")

        log.info("Sampling phase start")
        while (WORKER_STATE == SAMPLING_START) {Thread.sleep(500)}
        WorkerSampling.sendSampledDataToMaster()
        while (WORKER_STATE == SAMPLING_FINISH) {Thread.sleep(500)}
        log.info("Sampling phase stop")

        log.info("Sorting phase start")

        log.info("Sorting phase stop")
    }
}
