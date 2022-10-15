import HelperUtils.{ObtainConfigReference}
import MapReduceFuncs.MapReduce1.config
import com.mifmif.common.regex.Generex
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.PrivateMethodTester

import language.deprecated.symbolLiterals
import org.scalatest.matchers.should.Matchers

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.concurrent.Future.unit
import scala.util.matching.Regex

class Tests extends AnyFlatSpec with Matchers with PrivateMethodTester {
  behavior of "Map functions for each task"


  /*
  MapReduce1 tests
  */
  val config1 = ObtainConfigReference("MapReduce1") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  it should "check if time input falls withing specified time interval task 1" in {
    val startTimeStr = config1.getString("MapReduce1.StartTime")
    val endTimeStr = config1.getString("MapReduce1.EndTime")

    val format: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    val startTime = LocalTime.parse(startTimeStr, format)
    val endTime = LocalTime.parse(endTimeStr, format)

    val testTimeTrue = "00:47:10.000"
    val testTimeFalse = "00:47:00.000"

    val timeTrue = LocalTime.parse(testTimeTrue, format)
    val timeFalse = LocalTime.parse(testTimeFalse, format)
    assert(timeTrue.isAfter(startTime) && timeTrue.isBefore(endTime))
    assert(!(timeFalse.isAfter(startTime) && timeFalse.isBefore(endTime)))
  }

  it should "match regex string task 1" in {
    val patternStr = config1.getString("MapReduce1.Pattern")
    val pattern: Regex = patternStr.r
    val regexTrue = "gdhfghjgabcuihh"
    val regexFalse = "gdhfghjguihh"
    val isMatchTrue = pattern.findFirstMatchIn(regexTrue) match {
      case Some(_) => true
      case None => false
    }
    val isMatchFalse = pattern.findFirstMatchIn(regexFalse) match {
      case Some(_) => false
      case None => true
    }
    assert(isMatchTrue)
    assert(isMatchFalse)
  }


  /*
  MapReduce2 tests
  */
  val config2 = ObtainConfigReference("MapReduce2") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  it should "check if time input falls withing specified time interval task 2" in {
    val startTimeStr = config2.getString("MapReduce2.StartTime")
    val endTimeStr = config2.getString("MapReduce2.EndTime")

    val format: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    val startTime = LocalTime.parse(startTimeStr, format)
    val endTime = LocalTime.parse(endTimeStr, format)

    val testTimeTrue = "00:48:05.000"
    val testTimeFalse = "00:49:00.000"

    val timeTrue = LocalTime.parse(testTimeTrue, format)
    val timeFalse = LocalTime.parse(testTimeFalse, format)
    assert(timeTrue.isAfter(startTime) && timeTrue.isBefore(endTime))
    assert(!(timeFalse.isAfter(startTime) && timeFalse.isBefore(endTime)))
  }

  it should "check if log message is parse into just hours minutes and seconds task 2" in {
    val value = "00:47:05.312 [scala-execution-context-global-17] ERROR HelperUtils.Parameters$ - UG,Qe!&hgt|N[|aS/}kT#ZMs"
    val log = value.split(' ')
    val timeWithoutMs= log(0).split('.')(0)
    assert(timeWithoutMs == "00:47:05")
  }

  /*
  MapReduce3 tests
  */
  it should "check if log message can be split and log type can be extracted" in {
    val value = "00:47:05.312 [scala-execution-context-global-17] ERROR HelperUtils.Parameters$ - UG,Qe!&hgt|N[|aS/}kT#ZMs"
    val log = value.split(' ')
    assert(log(2) == "ERROR")
  }

  /*
  MapReduce4 tests
  */
  val config4 = ObtainConfigReference("MapReduce4") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  it should "match regex string task 4" in {
    val patternStr = config4.getString("MapReduce4.Pattern")
    val pattern: Regex = patternStr.r
    val regexTrue = "bcxvnvcbnibcvzbcv"
    val regexFalse = "vcxbcbvx"
    val isMatchTrue = pattern.findFirstMatchIn(regexTrue) match {
      case Some(_) => true
      case None => false
    }
    val isMatchFalse = pattern.findFirstMatchIn(regexFalse) match {
      case Some(_) => false
      case None => true
    }
    assert(isMatchTrue)
    assert(isMatchFalse)
  }





}