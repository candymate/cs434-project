import org.slf4j.LoggerFactory

import java.io._

object MergeUtil {
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
      
      mf1.removeFiles()
      mf2.removeFiles()

      println("merging done with files:")
      println(mfw.getFileList)

      new MultiFileRead(mfw.getFileList)
    }

    mfl match {
      case Nil => throw new IllegalArgumentException
      case e::Nil => e
      case _ => {
        val (lmfl, rmfl) = mfl.splitAt(mfl.size / 2)
        merge(mergeFiles(workDir, lmfl), mergeFiles(workDir, rmfl))
      }
    }
  }
}