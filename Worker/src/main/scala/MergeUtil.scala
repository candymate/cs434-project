import org.slf4j.LoggerFactory

import java.io._

object MergeUtil {
    val log = LoggerFactory.getLogger(getClass)

    def mergeFiles(workDir: File, mfl: List[MultiFileRead]): MultiFileRead = {
        assert(workDir.isDirectory)

        def merge(mf1: MultiFileRead, mf2: MultiFileRead): MultiFileRead = {
            val mfw = new MultiFileWrite(workDir.getAbsolutePath)

            var rec1 = mf1.readOneRecord
            var rec2 = mf2.readOneRecord
            while (rec1 != None || rec2 != None) {
                (rec1, rec2) match {
                    case (None, None) => throw new RuntimeException // should not happen
                    case (Some(r), None) => {
                        mfw.writeOneRecord(r)
                        rec1 = mf1.readOneRecord
                    }
                    case (None, Some(r)) => {
                        mfw.writeOneRecord(r)
                        rec2 = mf2.readOneRecord
                    }
                    case (Some(r1), Some(r2)) => {
                        if (r1.key < r2.key) {
                            mfw.writeOneRecord(r1)
                            rec1 = mf1.readOneRecord
                        }
                        else {
                            mfw.writeOneRecord(r2)
                            rec2 = mf2.readOneRecord
                        }
                    }
                }
            }

            mf1.close()
            mf2.close()
            mf1.removeFiles()
            mf2.removeFiles()

            log.info("merging done with files:" + mfw.getFileList.toString)
            
            mfw.close()
            new MultiFileRead(mfw.getFileList)
        }

        mfl match {
            case Nil => throw new IllegalArgumentException
            case e :: Nil => e
            case _ => {
                val (lmfl, rmfl) = mfl.splitAt(mfl.size / 2)
                merge(mergeFiles(workDir, lmfl), mergeFiles(workDir, rmfl))
            }
        }
    }

    def getListOfFiles(dir: File): List[File] = {
        if (dir.exists && dir.isDirectory) {
            dir.listFiles.filter(_.isFile).toList.sorted
        } else {
            List[File]()
        }
    }

    var fileNameCounter = 0

    def getFreshFileName(): String = {
        val fileName = "partition." + fileNameCounter.toString
        fileNameCounter += 1
        fileName
    }

    def renameFiles(dir: File): Unit = {
        getListOfFiles(dir).foreach(x => x.renameTo(new File(dir.getPath + "/" + getFreshFileName())))
    }
}