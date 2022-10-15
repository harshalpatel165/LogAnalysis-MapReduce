import HelperUtils.CreateLogger
import MapReduceFuncs.{MapReduce1, MapReduce2, MapReduce3, MapReduce4}

import scala.concurrent.{Await, Future, duration}
import concurrent.ExecutionContext.Implicits.global
import scala.annotation.meta.param
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class Application
object Application{
  def main(args: Array[String]): Unit = {
    val logger = CreateLogger(classOf[Application.type])
    logger.info("Application starting. Input 1 - 4 to run corresponding MR task:")
    logger.debug(args(1)) //input path
    logger.debug(args(2)) //output path and temporary input path for task 2
    //logger.debug(args(3)) //final output path for task 2

    args(0) match
      case "1" => MapReduce1.runMapReduce(args(1), args(2))
      case "2" => MapReduce2.runMapReduce(args(1), args(2), args(3))
      case "3" => MapReduce3.runMapReduce(args(1), args(2))
      case "4" => MapReduce4.runMapReduce(args(1), args(2))
      case _ => logger.info("Ending Program")
  }
}