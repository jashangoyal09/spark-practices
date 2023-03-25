package online.retail

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Start extends App {

  val sparkSession = SparkSession.builder().appName("spark-app").master("local").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val csv = "src/main/resources/online_retail_II.csv"

  // These are rules that helps to clean and add additional columns that are required
  val sanitizedRules = List(sanitizeData(_), addRevenue(_), addProductCode(_), addDateMonth(_))
  val dataframe = sanitizedRules.foldLeft(readCSV(sparkSession, csv))((y, x) => x(y))

  /**
   * Below code is fetching some insights from above dataframe.
   */
  dataframe.limit(100).show(false)
  calculateTotalRevenueByStockCode(dataframe).show(false)
  calTop10Products(dataframe).show(false)
  calMonthlyRevenue(dataframe).show(false)


  /**
   * This function reads csv file and create a dataframe
   * @param sparkSession
   * @param csvPath
   * @return dataframe
   */
  def readCSV(sparkSession: SparkSession, csvPath: String): DataFrame = {
    println(s"Reading data from CSV path $csvPath...")
    sparkSession.read.option("header", true).csv(csvPath)
  }

  /**
   * This function remove rows with any null value column and convert InvoiceDate type from string to timestamp.
   * @param dataFrame
   * @return dataframe after removing null value rows and changing type of InvoiceDate column
   */
  def sanitizeData(dataFrame: DataFrame): DataFrame = {
    println("Sanitizing Data...")
    dataFrame.na.drop().withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
  }

  /**
   * This function add new column i.e. revenue by multiplying quantity with price
   * @param dataFrame
   * @return dataframe with additional Revenue column
   */
  def addRevenue(dataFrame: DataFrame): DataFrame = {
    println("Adding Revenue to DF...")
    dataFrame.withColumn("Revenue", lit(col("Quantity") * col("Price")))
  }

  /**
   * This function get first three chars from StockCode column and created new column ProductCode with same values.
   * @param dataFrame
   * @return dataframe with new ProductCode column
   */
  def addProductCode(dataFrame: DataFrame): DataFrame = {
    println("Adding ProductCode column in dataframe...")
    dataFrame.withColumn("ProductCode", substring(col("StockCode"), 0, 3))
  }

  /**
   * This function fetched year and month from InvoiceDate column and creates new column InvoiceDate with year-month format.
   * @param dataFrame
   * @return dataframe with new InvoiceDate column
   */
  def addDateMonth(dataFrame: DataFrame): DataFrame = {
    println("Adding InvoiceDate column in dataframe...")
    dataFrame.withColumn("InvoiceMonth", concat(year(col("InvoiceDate")), lit("-"), month(col("InvoiceDate"))))
  }

  /**
   * This function calculates total of revenue by StockCode
   * @param revenueDF Dataframe with Revenue and StockCode columns
   * @return dataframe with StockCode and TotalRevenue columns
   */
  def calculateTotalRevenueByStockCode(revenueDF: DataFrame): DataFrame = {
    println("Calculating Total Revenue by Stock Code...")
    revenueDF.groupBy(col("StockCode")).agg(sum("revenue").alias("TotalRevenue"))
  }

  /**
   * This function calculates top 10 products by avg revenue of ProductCode column
   * @param revenueDf dataframe with ProductCode, StockCode, revenue columns
   * @return dataframe with ProductCode, Top10Products columns
   */

  def calTop10Products(revenueDf: DataFrame): DataFrame = {
    println("Calculating Top10Products in dataframe...")
    revenueDf.withColumn("ProductCode", substring(col("StockCode"), 0, 3)).
      groupBy(col("ProductCode")).agg(avg("revenue").alias("Top10Products")).orderBy(col("Top10Products").desc).limit(10)
  }

  /**
   * This function calculates monthly revenue by InvoiceMonth column
   * @param dateMonthDf dataframe with InvoiceMonth, revenue columns
   * @return dataframe with InvoiceMonth and TotalRevenue columns
   */
  def calMonthlyRevenue(dateMonthDf: DataFrame): DataFrame = {
    println("Calculating TotalRevenue by InvoiceMonth in dataframe...")
    dateMonthDf.groupBy("InvoiceMonth").agg(sum("revenue").alias("TotalRevenue"))
  }
}
