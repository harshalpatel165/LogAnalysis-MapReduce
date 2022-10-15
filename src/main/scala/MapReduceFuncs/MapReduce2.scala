package MapReduceFuncs

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable.Comparator
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import scala.jdk.CollectionConverters.IterableHasAsScala
import java.lang
import scala.jdk.CollectionConverters.*
import java.io.IOException
import java.util
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

import org.apache.hadoop.mapreduce.InputFormat

object MapReduce2:
  // Obtain reference to config file which contains predefined variables
  val config = ObtainConfigReference("MapReduce2") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  // Setup logging
  val logger = CreateLogger(classOf[MapReduce2.type])

  /*
  Mapper for first job that reads a log line and maps its timestamp to a count of 1 for each line that falls within specified
  time interval, contains specified regex pattern, and has log type of ERROR. The millisecond is not included
  to allow reducer to reduce output to a fixed time interval of 1 second
  Output example -
  12:00:00 1
  12:00:00 1
  12:00:00 1
  12:00:01 1
  12:00:01 1
  12:00:02 1
  ...
  */
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit =
      // Retrieve specified parameters
      val startTimeStr = config.getString("MapReduce2.StartTime")
      val endTimeStr = config.getString("MapReduce2.EndTime")
      val patternStr = config.getString("MapReduce2.Pattern")

      // Format time and regex to make it comparable
      val format: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(startTimeStr, format)
      val endTime = LocalTime.parse(endTimeStr, format)
      val pattern: Regex = patternStr.r
      logger.debug("test: " + startTime)
      logger.debug("test: " + endTime)

      // Split log line and extract its timestamp and message
      logger.debug("test map input: " + value)
      val log = value.toString.split(' ')
      val currTime = LocalTime.parse(log(0), format)
      val currMessage = log.last
      logger.debug("test current message: " + currMessage)
      logger.debug("test current time: " + currTime)

      // Test whether message contains specified regex
      val isMatch = pattern.findFirstMatchIn(currMessage) match{
        case Some(_) => true
        case None => false
      }

      // Test whether timestamp is within specified time interval and log type is ERROR
      if(currTime.isAfter(startTime) && currTime.isBefore(endTime) && isMatch && log(2) == "ERROR"){
        // Split timestamp from period to remove millisec. and return HH:mm:ss
        val timeWithoutMs= log(0).split('.')(0)
        word.set(timeWithoutMs)
        context.write(word, one)
        logger.info("Found match")
      }
      if (currTime.isBefore(startTime) && currTime.isAfter(endTime)) {
        logger.error("Timestamp out of interval")
      }
      logger.info("Task 2 job 1 map ran successfully!")

  /*
  Reduce for first job that aggregates timestamps by a fixed interval of 1 second
  with their count that is determined by the conditions mentioned in the mapper
  example output of reducer:
  12:00:01 6
  12:00:02 8
  12:00:03 4
  12:00:04 10
  ...
  */
  class Reduce1 extends Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      logger.debug("First reduce output key:" + key)
      logger.debug("First reduce output sum:" + sum)
      context.write(key, new IntWritable(sum.get()))
      logger.info("Task 2 job 1 reduce ran successfully!")

  /*
  Mapper for second job that swaps the timestamp with their count so that the
  SortComparator class can sort by key to sort the data by count of error logs
  Input of this mapper comes from output of reducer from 1 job.
  example output of this mapper:
  6 12:00:01
  8 12:00:02
  4 12:00:03
  10 12:00:04
  ...
  */
  class Swap extends Mapper[LongWritable, Text, IntWritable, Text] :
    private val timeInterval = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit =
      logger.info("test job 2 mapper input line" + value)
      val line = value.toString
      val kv = line.split("\\s+")

      logger.debug("test job 2 mapper extracted timestamp" + kv(0))
      logger.debug("test job 2 mapper extracted count" + kv(1))
      timeInterval.set(kv(0))
      context.write(new IntWritable(kv(1).toInt), timeInterval)
      logger.debug("Task 2 job 2 map ran successfully!")

  /*
  Reducer for second job that expands iterable list of timestamps that is
  the value to sorted keys representing the count
  input to this reducer comes from the comparator, example-
  10 [12:00:08, 12:00:03, 12:00:04]
  8 [12:00:02, 12:00:05]
  3 [12:00:01]
  1 [12:00:06, 12:00:07,]
  ...
  example output of reducer:
  12:00:08 10
  12:00:03 10
  12:00:04 10
  12:00:02 8
  12:00:05 8
  12:00:01 3
  ...
  */
  class ReSwap extends Reducer[IntWritable, Text, Text, IntWritable] {
    override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      values.asScala.foreach((value: Text) => {
        // count and timestamp again as well as expands list of timestamps for each key
        context.write(value, key)
      })
      logger.debug("Task 2 job 2 reduce ran successfully!")
    }
  }

  // Sorts output of 2nd mapper by key which is the count and aggregates all timestamps for that count
  // and is sent to reducer
  class SortComparator extends WritableComparator(classOf[IntWritable], true) {
    override def compare(str1: WritableComparable[_], str2: WritableComparable[_]): Int = {
      val count1 = str1.asInstanceOf[IntWritable]
      val count2 = str2.asInstanceOf[IntWritable]
      // compareTo will return a negative value if count2 is less than count1
      // it is then multiplied by -1 to make it positive giving it precedence over count 1
      -1 * count1.compareTo(count2)
    }
  }

  /*
  Sets up and Initiates MapReduce jobs.
  The first job maps data and reduces the timestamps and count.
  Second job sorts and reformat output.
  */
  def runMapReduce(inputPath: String, outputPath: String, outputPath2: String) =
    //require(!inputPath.isBlank() && !outputPath.isBlank())
    logger.info("Input path recieved by MapReduce Task 2:" + inputPath)
    logger.info("Output path recieved by MapReduce Task 2:" + outputPath)
    logger.info("Final output path recieved by MapReduce Task 2:" + outputPath2)
    val conf1 = new Configuration // new job configuration
    conf1.set("mapreduce.job.reduces", "1")
    conf1.set("mapreduce.job.maps", "1")
    // creates new job with specified configuration
    val job1 = Job.getInstance(conf1,"job1")
    job1.setJobName("MapReduce2 job1") // sets job name
    job1.setJarByClass(this.getClass)
    job1.setOutputKeyClass(classOf[Text]) // set type for key of the output of this job.
    job1.setOutputValueClass(classOf[IntWritable]) // set type for value of the output of this job.
    job1.setMapOutputKeyClass(classOf[Text]) // set type for key of output of the mapper
    job1.setMapOutputValueClass(classOf[IntWritable]) // set type for value of output of the mapper
    job1.setInputFormatClass(classOf[TextInputFormat]) // default to key being position in file and value being the line of text
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    // Set defined Mapper class that maps timestamps with 1 second intervals to count of ERROR log types and other conditions
    job1.setMapperClass(classOf[Map])
    //conf.setCombinerClass(classOf[Reduce])
    // Set defined Reducer class that aggregates counts for those timestamps
    job1.setReducerClass(classOf[Reduce1])

    // Set input path and output path. Output path is the input path for that next job
    val inpath: Path = new Path(inputPath)
    val outpath: Path = new Path(outputPath)
    FileInputFormat.addInputPath(job1, inpath)
    FileOutputFormat.setOutputPath(job1, outpath)
    // Wait for job to complete before starting next one to prevent incorrect outputs from being generated
    job1.waitForCompletion(true)
    logger.info("Completed Job 1!")

    /*
    Second job that takes input from 1st job reduce and swaps key and value in mapper called Swap.
    Comparator class sorts by key which is now changed due to swap to counts instead of timestamps.
    Comparator sends sorted keys and aggregated timestamps for that each key to reducer.
    Reducer called ReSwap expands that list of timestamps for each particular key(count) that is sorted
    and also re swaps count and timestamp
    */
    val conf2 = new Configuration
    conf2.set("mapreduce.job.maps", "1")
    conf2.set("mapreduce.job.reduces", "1")
    //conf2.set("mapreduce.output.textoutputformat.separator", ",")
    val job2 = Job.getInstance(conf2, "job2")
    job2.setJobName("MapReduce2 job2")

    job2.setJarByClass(this.getClass)
    job2.setOutputKeyClass(classOf[IntWritable])
    job2.setOutputValueClass(classOf[Text])
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])

    job2.setMapperClass(classOf[Swap])
    job2.setSortComparatorClass(classOf[SortComparator]) // Sorter
//    job2.setNumReduceTasks(0)
    job2.setReducerClass(classOf[ReSwap])
    job2.setInputFormatClass(classOf[TextInputFormat])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    //input to second job is the ouput from reducer of first job
    //final output of Task 2 is stored in outpath2
    val inpath2: Path = new Path(outputPath)
    val outpath2: Path = new Path(outputPath2)
    FileInputFormat.addInputPath(job2, inpath2)
    FileOutputFormat.setOutputPath(job2, outpath2)

    job2.waitForCompletion(true)

    logger.info("Completed Job 2!")
