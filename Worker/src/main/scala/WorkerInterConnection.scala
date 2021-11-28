import Worker.WORKER_STATE
import WorkerState.SHUFFLE_FINISH
import channel._
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder}
import protobuf.connect._
import scala.concurrent.Future
import java.util.concurrent.locks.ReentrantLock
import org.slf4j.{Logger, LoggerFactory}
import java.io.{BufferedWriter, File, FileWriter, IOException}
import scala.io.Source

object WorkerInterRequest {
    val log: Logger = LoggerFactory.getLogger(getClass)
    var fileNamePartition = 0

    def sendInterShuffle(ip: String, port: Int, idx: Int): Unit = {
        assert(WORKER_STATE == SHUFFLE_FINISH)
        assert(ip != null)

        val managedChannelBuilder = ManagedChannelBuilder.forAddress(ip, port)
        managedChannelBuilder.usePlaintext()
        managedChannelBuilder.maxInboundMessageSize(64*1000*1000) // max response size to 64MB
        val channel = managedChannelBuilder.build()
        log.info("Interconnection channel built for " + ip + ":" + port)

        var req: ShufflingInterRequest.ReqType = ShufflingInterRequest.ReqType.NEXT
        val blockingStub = shuffleInterWorkerGrpc.blockingStub(channel)
        while ({
            val response = try {
                blockingStub.interShuffling(new ShufflingInterRequest(req, idx))
            } catch {
                case e: StatusRuntimeException => {
                    sys.exit(1)
                }
            }
            
            if (response.status) {
                assert(response.fileContent != "")

                try {
                    val filePath = WorkerArgumentHandler.outputFile.getPath + "/" + generateName()
                    val bufferedWriter = new BufferedWriter(new FileWriter(new File(filePath)))
                    bufferedWriter.write(response.fileContent + "\r\n")
                    bufferedWriter.close()
                    req = ShufflingInterRequest.ReqType.NEXT
                }
                catch {
                    case e: IOException => {
                        fileNamePartition -= 1
                        req = ShufflingInterRequest.ReqType.RETRY
                    }
                }
            }

            response.status
        }) ()

        try {
            val response = blockingStub.interShuffling(new ShufflingInterRequest(ShufflingInterRequest.ReqType.OK, idx))
            assert(!response.status && response.fileContent == "")
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }

        log.info("Interconnection channel shutdown for " + ip + ":" + port)
        channel.shutdown()
    }

    private def generateName(): String = {
        val newName = "unmerged." + fileNamePartition
        fileNamePartition += 1
        newName
    }
}

class WorkerInterService extends shuffleInterWorkerGrpc.shuffleInterWorker {
    val log: Logger = LoggerFactory.getLogger(getClass)
    var fileIndices: List[Int] = Nil
    var fileList: List[List[File]] = Nil
    private val lock = new ReentrantLock()

    override def interShuffling(request: ShufflingInterRequest): Future[ShufflingInterResponse] = synchronized {
        assert(WORKER_STATE == SHUFFLE_FINISH)

        lock.lock()
        val workerNum = WorkerToWorkerChannel.ipList.size
        assert(workerNum > 0)

        if (fileList == Nil) { fileList = getFileList() }
        if (fileIndices == Nil) { fileIndices = List.fill[Int](workerNum)(0) }

        assert(request.id < workerNum && request.id >= 0)
        assert(fileIndices(request.id) != -1)

        var status = false
        var fileContent = ""
        try {
            request.req match {
                case ShufflingInterRequest.ReqType.NEXT => {
                    assert(!fileList(request.id).isEmpty && fileIndices(request.id) <= fileList(request.id).size)

                    if (fileIndices(request.id) == fileList(request.id).size) { // no more to send
                        val f = fileList(request.id)(fileIndices(request.id) - 1)
                        assert(f.exists && f.isFile)
                        assert(f.delete())

                        status = false
                        fileContent = ""
                    }
                    else {
                        val f = fileList(request.id)(fileIndices(request.id))
                        assert(f.exists && f.isFile)

                        fileContent = Source.fromFile(f).getLines.mkString("\r\n")
                        
                        if (fileIndices(request.id) > 0) {
                            val f = fileList(request.id)(fileIndices(request.id) - 1)
                            assert(f.exists && f.isFile)
                            assert(f.delete())
                        }

                        fileIndices = fileIndices.updated(request.id, fileIndices(request.id) + 1)

                        status = true
                    }
                }
                case ShufflingInterRequest.ReqType.RETRY => {
                    assert(!fileList(request.id).isEmpty && fileIndices(request.id) <= fileList(request.id).size)
                    assert(fileIndices(request.id) > 0) // at least 1

                    val f = fileList(request.id)(fileIndices(request.id) - 1)
                    assert(f.exists && f.isFile)

                    fileContent = Source.fromFile(f).getLines.mkString("\r\n")
                    status = true
                }
                case ShufflingInterRequest.ReqType.OK => {
                    fileIndices = fileIndices.updated(request.id, -1)

                    status = false
                    fileContent = ""
                }

            }
        } finally {
            lock.unlock()
        }

        Future.successful {
            ShufflingInterResponse(status, fileContent)
        }
    }

    def getFileList(): List[List[File]] = {
        assert(fileList == Nil)

        val outputPathFile = WorkerArgumentHandler.outputFile
        assert(outputPathFile.exists && outputPathFile.isDirectory)
        
        val outputFileList = outputPathFile.listFiles.filter(_.isFile).toList.sorted
        val workerNum = WorkerToWorkerChannel.ipList.size
        assert(workerNum > 0)

        (0 until workerNum).toList.map(i => outputFileList.filter(f => f.getName.contains("unshuffled." + i + ".")))
    }
}
