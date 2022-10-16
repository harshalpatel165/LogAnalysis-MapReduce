package MapReduceFuncs

import HelperUtils.{CreateLogger, ObtainConfigReference}
import MapReduceFuncs.MapReduce2.logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.log4j.helpers.DateTimeDateFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import scala.jdk.CollectionConverters.*
import java.io.IOException
import java.{lang, util}
import java.text.SimpleDateFormat
import java.text.DateFormat
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

object MapReduce1:
  // Obtain reference to config file which contains predefined variables
  val config = ObtainConfigReference("MapReduce1") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  // Setup logging
  val logger = CreateLogger(classOf[MapReduce1.type])

  /*
  Mapper that reads a log line and maps its log type to a count of 1 for each line that falls within specified
  time interval and contains specified regex pattern
  Output example -
  ERROR 1
  ERROR 1
  WARN 1
  INFO 1
  INFO 1
  ...
  */
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit =
      // Retrieve specified parameters
      val startTimeStr = config.getString("MapReduce1.StartTime")
      val endTimeStr = config.getString("MapReduce1.EndTime")
      val patternStr = config.getString("MapReduce1.Pattern")

      // Format time and regex to make it comparable
      val format: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(startTimeStr, format)
      val endTime = LocalTime.parse(endTimeStr, format)
      val pattern: Regex = patternStr.r
      logger.debug("test start time: " + startTime)
      logger.debug("test end time: " + endTime)

      // Split log line and extract its timestamp and message
      logger.debug("test map input: " + value)
      val log = value.toString.split(' ')
      val currTime = LocalTime.parse(log(0), format)
      val currMessage = log.last
      logger.debug("test current message: " + currMessage)
      logger.debug("test current time: " + currTime)

      // Test whether message contains specified regex
      val isMatch = pattern.findFirstMatchIn(currMessage) match {
        case Some(_) => true
        case None => false
      }

      // Test whether timestamp is within specified time interval
      if(currTime.isAfter(startTime) && currTime.isBefore(endTime) && isMatch){
        word.set(log(2))
        context.write(word, one)
        logger.info("Found match")
      }

      // If timestamp outside of interval abounds, report error
      if(currTime.isBefore(startTime) && currTime.isAfter(endTime)){
        logger.error("Timestamp out of interval")
      }
      logger.info("Task 1 map ran successfully!")

  
  
  /* Aggregate log types and their counts.
  Receives input from the output of the mapper and outputs 4 lines, example-
  ERROR 2
  WARN 1
  INFO 2
  DEBUG 3
  */
  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      context.write(key, new IntWritable(sum.get()))
      logger.info("Task 1 reduce ran successfully!")

  // Sets up and Initiates MapReduce job
  def runMapReduce(inputPath: String, outputPath: String) =
    //require(!inputPath.isBlank() && !outputPath.isBlank())
    logger.info("Input path recieved by MapReduce Task 1:" + inputPath)
    logger.info("Output path recieved by MapReduce Task 1:" + outputPath)
    val conf1 = new Configuration
    conf1.set("mapreduce.output.textoutputformat.separator", ", ")
    conf1.set("mapreduce.job.reduces", "1")
    val job1 = Job.getInstance(conf1, "job1")
    job1.setJobName("MapReduce1")
    job1.setJarByClass(this.getClass)
    job1.setOutputKeyClass(classOf[Text]) // set type for key of the output. Text to represent log type ie. WARN, DEBUG etc.
    job1.setOutputValueClass(classOf[IntWritable]) // set type for value of the output. IntWritable to represent count
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[IntWritable])
    job1.setMapperClass(classOf[Map]) // Set defined Mapper class

    //conf.setCombinerClass(classOf[Reduce])
    job1.setReducerClass(classOf[Reduce]) // Set defined Reducer class
    job1.setInputFormatClass(classOf[TextInputFormat]) // default to key being position in file and value being the line of text
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    val inpath: Path = new Path(inputPath)
    val outpath: Path = new Path(outputPath)
    FileInputFormat.addInputPath(job1, inpath) // add input and output paths from arguments
    FileOutputFormat.setOutputPath(job1, outpath)
    job1.waitForCompletion(true)  // run mapreduce job
    logger.info("Task 1 job completed!")