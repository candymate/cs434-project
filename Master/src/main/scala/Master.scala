import MasterState._
import config.ClientInfo
import scala.collection.mutable
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Master {
    @volatile var MASTER_STATE: MasterState = CONNECTION_START
    
    // key: machine order, value: ClientInfo
    var clientInfoMap: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()

    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        log.info("Handling argument")
        MasterArgumentHandler.handleArgument(args)

        log.info("Connection phase start")
        val connectionClass = new MasterConnection(MasterArgumentHandler.slaveNum, ExecutionContext.global)

        log.info(s"started master server expecting ${MasterArgumentHandler.slaveNum} slave(s)")
        connectionClass.start()

        log.info("Stopping main function until connection phase is completed")
        while (MASTER_STATE == CONNECTION_START) {Thread.sleep(500)}

        connectionClass.broadcastConnectionIsFinished()
        log.info("Connection phase successfully finished")

        log.info("Sampling phase start")
        val samplingClass = new MasterSampleRequest(null)
        samplingClass.sendSampleRequestToEveryClient()

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
