package MapReduceFuncs

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.log4j.helpers.DateTimeDateFormat

import scala.jdk.CollectionConverters.*
import java.io.IOException
import java.{lang, util}
import java.text.SimpleDateFormat
import java.text.DateFormat
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

object MapReduce4:
  // Obtain reference to config file which contains predefined variables
  val config = ObtainConfigReference("MapReduce4") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  // Setup logging
  val logger = CreateLogger(classOf[MapReduce4.type])
  /*
  Mapper that reads a log line and maps its log type to a count of log message length
  for each line that contains specified regex pattern
  Output example -
  ERROR 30
  ERROR 15
  WARN 54
  WARN 68
  INFO 12
  INFO 42
  ...
  */
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] :
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit =
      // Retrieve specified parameters
      val patternStr = config.getString("MapReduce4.Pattern")
      // Split log line and extract its message and message size
      logger.debug("test map input: " + value)
      val log = value.toString.split(' ')
      val currMessage = log.last;
      val currMessageSize = log.last.size;
      logger.debug("test current message: " + currMessage)
      logger.debug("test current message size: " + currMessageSize)
      val pattern: Regex = patternStr.r

      // Test whether message contains specified regex
      val isMatch = pattern.findFirstMatchIn(currMessage) match {
        case Some(_) => true
        case None => false
      }
      
      //if it log message contains specified regex,
      //map its log type to the number of characters in the message
      if(isMatch){
        word.set(log(2))
        context.write(word, new IntWritable(currMessageSize))
        logger.info("Found match")
      }
      logger.info("Task 4 map ran successfully!")
      
  /* 
  Aggregate and determines the largest message length for each type.
  Receives input from the output of the mapper and outputs 4 lines, example-
  ERROR 30
  WARN 68
  INFO 42
  DEBUG 15
  */
  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit =
      val maximum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(Math.max(valueOne.get(), valueTwo.get()))) // return bigger message length
      context.write(key, new IntWritable(maximum.get()))
      logger.info("Task 4 reduce ran successfully!")

  // Sets up and Initiates MapReduce job
  def runMapReduce(inputPath: String, outputPath: String) =
    //require(!inputPath.isBlank() && !outputPath.isBlank())
    logger.info("Input path recieved by MapReduce Task 4:" + inputPath)
    logger.info("Output path recieved by MapReduce Task 4:" + outputPath)
    //conf1.set("mapreduce.output.textoutputformat.separator", ",")
    val conf1 = new Configuration // new job configuration
    conf1.set("mapreduce.job.reduces", "1")
    val job1 = Job.getInstance(conf1, "job1") // new job configuration
    job1.setJobName("MapReduce4")
    job1.setJarByClass(this.getClass)

    job1.setOutputKeyClass(classOf[Text]) // set type for key of the output. Text to represent log type ie. WARN, DEBUG etc.
    job1.setOutputValueClass(classOf[IntWritable]) // set type for value of the output. IntWritable to represent max message length
    job1.setMapperClass(classOf[Map]) // Set defined Mapper class
    //conf.setCombinerClass(classOf[Reduce])
    job1.setReducerClass(classOf[Reduce]) // Set defined Reducer class
    job1.setInputFormatClass(classOf[TextInputFormat]) // default to key being position in file and value being the line of text
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    val inpath: Path = new Path(inputPath)
    val outpath: Path = new Path(outputPath)
    FileInputFormat.setInputPaths(job1, inpath) // add input and output paths from arguments
    FileOutputFormat.setOutputPath(job1, outpath)
    job1.waitForCompletion(true) // run mapreduce job
