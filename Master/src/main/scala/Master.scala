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
        val samplingClass = new MasterSampleStartRequest(null)
        samplingClass.broadcastSampleStart()

        while (MASTER_STATE == SAMPLING_START) {Thread.sleep(500)}
        log.info("Sampling phase connection phase finished")

        // sort records
        log.info("Sorting sampled records start")
        // val sortSampledRecords = new MasterSortSampledRecords(samplingClass.sampledData, MasterArgumentHandler.slaveNum)
        log.info("Sorting sampled records finished")

        // sort/partitioning phase (sever not required in master)
        log.info("Sorting phase start")
        //val sortingClass = new MasterSampleSortRequest(connectionClass.clientInfoMap, null,
        //    sortSampledRecords.pivotList)
        // sortingClass.sendSortRequestToEveryClient()
        log.info("Sorting phase stop")

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
