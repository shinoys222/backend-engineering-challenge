import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import scopt.OParser
import java.nio.file.{Paths, Files}

case class Config(
    windowSize: Int = 10,
    inputDirectory: String = "./input_data",
    outputDirectory: String = "./output/output.json"
  )


object Main extends App {

  var args_config = Config()

  // Command Line Args Parser based on scopt library
  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("Unbabel Aggregator"),
      head("Unbabel Aggregator", "1.0"),

      opt[Int]('w', "window_size")
        .required()
        .action((x, c) => c.copy(windowSize = x.toInt))
        .text("foo is an integer property"),

      opt[String]('i', "input_directory")
        .required()
        .valueName("<input directory>")
        .action((x, c) => c.copy(inputDirectory = x)),


      opt[String]('o', "output_directory")
        .required()
        .valueName("<output directory>")
        .action((x, c) => c.copy(outputDirectory = x))

    )
  }

  // OParser.parse returns Option[Config]
  OParser.parse(parser1, args, Config()) match {
    case Some(args) =>
      args_config = args
    case _ =>
      // arguments are bad, error message will have been displayed
      sys.exit()

  }

  val input_directory_exists = Files.exists(Paths.get(args_config.inputDirectory))
  val is_file = Files.isRegularFile(Paths.get(args_config.inputDirectory))
  if(is_file || !input_directory_exists){
    println(args_config.inputDirectory + " is not a directory")
    sys.exit()

  }

  val window_time = args_config.windowSize.toString + " minutes"

  // Defining input json schema. A schema is necessary for file sources
  val inputJsonSchema = new StructType()
    .add("timestamp", TimestampType)
    .add("translation_id", StringType)
    .add("source_language", StringType)
    .add("target_language", StringType)
    .add("client_name", StringType)
    .add("event_name", StringType)
    .add("nr_words", IntegerType)
    .add("duration", DoubleType)

  // Setting all spark log levels to Error so that only errors shhow up in console
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  // Initializing spark streaming session in localhost
  val spark = SparkSession
    .builder
    .appName("UnbabelWindowAggregator")
    .config("spark.master", "local")
    .getOrCreate()

  // Importing spark session's implicit functions and syntax
  import spark.implicits._

  /* 
  Using file source to input data directory. File source only supports directory and reads all the json files inside the directory.
  we need to specify the schema.
  */
  val input_df = spark.readStream
    .schema(inputJsonSchema)
    .format("json")
    .option("maxFilesPerTrigger", 1) 
    .load(args_config.inputDirectory)
    .withWatermark("timestamp", "1 second") 
  /* 
     Watermark allows to persist the events in state. 
     The first parameter is the the timstamp field/column of timestamp Datatype 
     and 2nd Param is the threshold interval to persist items in memory for aggregarions.
     Late data within the threshold will be aggregated, but data later than the threshold will start getting dropped
     For more info visit 
     https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
  */

  /*
    We are grouping data based on time window using window function provided by spark.
    This will create a window field/column(12:00-12:10, 12:01-12:11 ..etc) 
    and the last parameter decide the sliding/rolling time between 2 windows, 
    if none is specified it will not use any sliding windows, will use per window_time.
    Aggregate function of mean is used on the grouped data to calculate the average. 
  */
  val output_df = input_df
    .groupBy(window($"timestamp", window_time, "1 minute"))
    .agg(mean("duration").as("average_delivery_time"))
    .select($"window.end".alias("date"), $"average_delivery_time")

  /*
    FileSink is used to store the results. The output will be stored in a file directory as multiple files.
    FileSink only supports append mode and only watermarked Dataframes/Datasets is supported.
    Checkpoint directory is necessary to store the checkpoints so that the application can continue next time from where it is interrupted
  */
  val query = output_df.writeStream
    .outputMode("append")
    .format("json")
    .option("path", args_config.outputDirectory) 
    .option("checkpointLocation", "checkpoint-dir")
    .start()

  /*
    Console is also used since the output of the file sink is spread across multiple files and is difficult to read
  */
  val consoleQuery = output_df.sort($"date")
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  consoleQuery.awaitTermination

}