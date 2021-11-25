import config.MasterConfig
import org.slf4j.LoggerFactory

import java.io.File
import scala.concurrent.ExecutionContext

object Worker {
    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        // argument handling
        val masterIpPortInfo: MasterConfig = new MasterConfig(args(0).split(":")(0),
            args(0).split(":")(1).toInt)
        var inputFilePathList: Array[File] = Array()
        args.slice(2, args.length - 2).foreach{ case filePath: String => {
            val directory = new File(filePath)
            if (directory.isDirectory && directory.exists()) {
                inputFilePathList = inputFilePathList ++ directory.listFiles(_.isFile)
            }
        }}

        val outputFilePath: File = new File(args.last)

        // connection phase (server not required in worker)
        log.info("Connection phase start")
        new WorkerConnection(masterIpPortInfo, null)
        log.info("Connection phase successfully finished")

        // worker server start
        log.info("Worker Server start for communication")
        val workerServer = new WorkerServer(inputFilePathList, outputFilePath, ExecutionContext.global)
        log.info("Worker Server start completed for communication")

        // sampling phase (server required in worker)

        // shuffling (depending on implementation)

        // merging phase (server required in worker)

        // checking (server required in worker)
    }
}
