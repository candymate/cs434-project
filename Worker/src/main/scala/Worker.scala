import config.MasterConfig
import org.slf4j.LoggerFactory

import java.io.File

object Worker {
    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        // argument handling
        val masterIpPortInfo: MasterConfig = new MasterConfig(args(0).split(":")(0),
            args(0).split(":")(1).toInt)
        val inputFilePathList: Array[File] = args.slice(2, args.length - 2).map{ case filePath: String => new File(filePath) }
        val outputFilePath: File = new File(args.last)

        // worker server start
        log.info("Worker Server start for communication")
        val workerServer = new WorkerServer()
        log.info("Worker Server start completed for communication")

        // connection phase (server not required in worker)
        log.info("Connection phase start")
        new WorkerConnection(masterIpPortInfo, null)
        log.info("Connection phase successfully finished")

        // sampling phase (server required in worker)

        // shuffling (depending on implementation)

        // merging phase (server required in worker)

        // checking (server required in worker)
    }
}
