import channel.WorkerToMasterChannel
import config.MasterConfig
import org.slf4j.LoggerFactory

import java.io.File

object WorkerArgumentHandler {
    val log = LoggerFactory.getLogger(getClass)
    var ip: String = null
    var port: Int = 0
    var inputFileArray: Array[File] = Array()
    var outputFile: File = null
    var optionalWorkerServerPort: Int = 8000

    def handleArgument(args: Array[String]): Unit = {
        ip = args(0).split(":")(0)
        port = args(0).split(":")(1).toInt

        WorkerToMasterChannel.configureMasterIpAndPort(ip, port)

        args.slice(2, args.length - 2).foreach {
            case filePath: String => {
                val directory = new File(filePath)
                if (directory.isDirectory && directory.exists()) {
                    inputFileArray = inputFileArray ++ directory.listFiles(_.isFile)
                }
            }
        }

        if (args(args.length - 2).equals("-O")) {
            log.info("Optional port for worker server is not included in args")
            outputFile = new File(args.last)
        } else {
            log.info("Optional port for worker server is included in args")
            optionalWorkerServerPort = args.last.toInt
            outputFile = new File(args(args.length - 1))
        }
    }

}
