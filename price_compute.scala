package project.etf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object price_compute extends  App{


  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("ETFQuarterlyAnalysis")
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Use legacy time parser
    .getOrCreate()


  // Read the CSV file
  //PATH TO LOCAL
  //val inputFilePath = "C:\\Users\\Ian Mbaya\\BD_Training\\Project\\DataFile\\Staged\\Batch\\prices_batch_1.csv"
  // Path to HDFS
  val inputFilePath = "/tmp/ian/project/etf_prices/"

  // Specify the expected schema (column names) if the CSV does not have headers
  val columnNames = Seq("fund_symbol", "price_date", "open", "high", "low", "close", "adj_close", "volume")

  // Read the CSV file without header and assign custom column names
    val etfDF = spark.read
    .option("header", "false") // Indicate that the CSV does not have headers
    .option("inferSchema", "true")
    .csv(inputFilePath)
    .toDF(columnNames: _*) // Assign custom column names





  // Convert the price_date to a proper date format and create a quarter column
  val etfWithQuarter = etfDF.withColumn("price_date", to_date(col("price_date"), "MM/dd/yyyy"))
    .withColumn("quarter", concat(year(col("price_date")), lit("-Q"), quarter(col("price_date"))))

  // Group the data by fund_symbol and quarter to calculate the required metrics
  val aggregatedDF = etfWithQuarter
    .groupBy("fund_symbol", "quarter")
    .agg(
      avg("volume").alias("avg_volume"),
      max("high").alias("high"),
      min("low").alias("low"),
      first("close").alias("start_close"), // Get the first close price of the quarter
      last("close").alias("end_close")     // Get the last close price of the quarter
    )

  // Calculate the ROI and absolute return
  val resultWithReturns = aggregatedDF
    .withColumn("roi", expr("(end_close - start_close) / start_close * 100")) // ROI calculation
    .withColumn("absolute_return", expr("end_close - start_close"))            // Absolute return calculation

  // Define a window specification for calculating the moving average
  val windowSpec = Window.partitionBy("fund_symbol").orderBy("quarter").rowsBetween(-3, 0)

  // Add moving average for close prices to the original DataFrame
  val etfWithMovingAvg = etfWithQuarter
    .withColumn("quarter", concat(year(col("price_date")), lit("-Q"), quarter(col("price_date"))))
    .withColumn("close_moving_avg", avg("close").over(windowSpec)) // Calculate moving average over the window

  // Now group the data by fund_symbol and quarter again to include the moving average
  val finalAggregatedDF = etfWithMovingAvg
    .groupBy("fund_symbol", "quarter")
    .agg(
      avg("volume").alias("avg_volume"),
      max("high").alias("high"),
      min("low").alias("low"),
      first("close").alias("start_close"), // Get the first close price of the quarter
      last("close").alias("end_close"),     // Get the last close price of the quarter
      avg("close_moving_avg").alias("close_moving_avg") // Include moving average
    )

  // Calculate ROI and absolute return in the final result
  val finalResultDF = finalAggregatedDF
    .withColumn("roi", expr("(end_close - start_close) / start_close * 100")) // ROI calculation
    .withColumn("absolute_return", expr("end_close - start_close")) // Absolute return calculation

  // Local output path
  // val outputFilePath = "C:\\Users\\Ian Mbaya\\BD_Training\\Project\\DataFile\\hive_replica"
  // Path to HDFS
  val outputFilePath = "/tmp/ian/project/quarterly.csv"

  // Write the result to a new CSV file
  finalResultDF.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputFilePath)

  // Stop the Spark session
  spark.stop()



}
