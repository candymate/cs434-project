import MasterState._
import config.ClientInfo
import scala.collection.mutable
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Master {
    @volatile var MASTER_STATE: MasterState = CONNECTION_START
    
    // key: machine order, value: ClientInfo
    var clientInfoMap: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()
    var numOfRequiredConnections: Int = 0

    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        log.info("Handling argument")
        MasterArgumentHandler.handleArgument(args)

        log.info("Start master server")
        val masterServer = new MasterServer(ExecutionContext.global)
        masterServer.start()
        assert(numOfRequiredConnections > 0)
        log.info(s"started master server expecting ${numOfRequiredConnections} slave(s)")
        log.info("Successfully turned on master side server")

        log.info("Connection phase start")
        log.info("Stopping main function until connection phase is completed")
        while (MASTER_STATE == CONNECTION_START) {Thread.sleep(500)}
        val connectionClass = new MasterConnectionDoneRequest(null)
        connectionClass.broadcastConnectionDone()
        log.info("Connection phase successfully finished")

        log.info("Sampling phase start")
        val samplingClass = new Request_MasterSample(null)
        samplingClass.broadcastSampleStart()
        while (MASTER_STATE == SAMPLING_START) {Thread.sleep(500)}
        MasterSortSampledRecords.pivotFromSampledRecords(Service_MasterSample.sampledRecords)
        while (MASTER_STATE == SAMPLING_FINISH) {Thread.sleep(500)}
        log.info("Sampling phase connection phase finished")

        // sort records
        log.info("Sorting sampled records start")
        val sortingClass = new Request_MasterSort(null)
        sortingClass.broadcastSortStart()
        while (MASTER_STATE == SORT_PARTITION_START) {Thread.sleep(500)}
        while (MASTER_STATE == SORT_PARTITION_FINISH) {Thread.sleep(500)}
        log.info("Sorting sampled records finished")

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
