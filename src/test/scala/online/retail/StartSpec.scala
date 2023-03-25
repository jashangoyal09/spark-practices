package online.retail

import online.retail.Start._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers._

class StartSpec extends AnyFunSuite with should.Matchers {

  val sparkSession = SparkSession.builder().appName("spark-test-app").master("local").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val data = List(("489434", "85048", "15CM CHRISTMAS GLASS BALL 20 LIGHTS", "12", "1/1/2009 7:45", "6.95", "13085", "United Kingdom"),
    ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "12/31/2009 7:45", "6.75", "13085", "United Kingdom"),
    ("489434", "79323P", "PINK CHERRY LIGHTS", "120", "12/1/2009 7:45", "1", "13085", "United Kingdom"),
    ("489434", "79323W", "WHITE CHERRY LIGHTS", "0", "12/1/2009 7:45", "6.75", "13085", "United Kingdom"),
    ("489434", "22041", "RECORD FRAME 7 SINGLE SIZE", "48", "12/1/2009 7:45", "2.1", "13085", "United Kingdom"),
    ("489434", "21232", "STRAWBERRY CERAMIC TRINKET BOX", "24", "12/1/2009 7:45", "1.25", "13085", "United Kingdom"),
    ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom"),
    ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom"),
    ("489434", "22", "PINK DOUGHNUT TRINKET POT", "24", "05/12/2009 7:45", "1.65", "13085", "United Kingdom"),
    ("489434", "22064", "PINK DOUGHNUT TRINKET POT", null, "12/1/2009 7:45", "1.65", "13085", "United Kingdom"))

  val df = data.toDF("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country")

  test("Should remove rows containing null values") {
    val dataSetCount = sanitizeData(df).count
    assert(dataSetCount === 9)
  }

  test("Should change type of InvoiceDate to timestamp") {
    val data = List(("489434", "85048", "15CM CHRISTMAS GLASS BALL 20 LIGHTS", "12", "2009-01-01 07:45:00", "6.95", "13085", "United Kingdom"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "2009-12-31 07:45:00", "6.75", "13085", "United Kingdom"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "120", "2009-12-01 07:45:00", "1", "13085", "United Kingdom"),
      ("489434", "79323W", "WHITE CHERRY LIGHTS", "0", "2009-12-01 07:45:00", "6.75", "13085", "United Kingdom"),
      ("489434", "22041", "RECORD FRAME 7 SINGLE SIZE", "48", "2009-12-01 07:45:00", "2.1", "13085", "United Kingdom"),
      ("489434", "21232", "STRAWBERRY CERAMIC TRINKET BOX", "24", "2009-12-01 07:45:00", "1.25", "13085", "United Kingdom"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "2009-12-01 07:45:00", "1.65", "13085", "United Kingdom"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "2009-12-01 07:45:00", "1.65", "13085", "United Kingdom"),
      ("489434", "22", "PINK DOUGHNUT TRINKET POT", "24", "2009-05-12 07:45:00", "1.65", "13085", "United Kingdom"))

    val expectedDf = data.toDF("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country").
      withColumn("InvoiceDate", col("InvoiceDate").cast(TimestampType))
    val sanitizedDF = sanitizeData(df)

    sanitizedDF.show(false)
    expectedDf.show(false)

    sanitizedDF.collect.toList should contain theSameElementsAs expectedDf.collect.toList
  }

  test("Should Add Revenue column in the dataset") {
    val revenueDF = addRevenue(df)
    assert(revenueDF.columns.size === df.columns.size + 1)
  }

  test("Should calculate Revenue column by multiply quantity and unit price") {
    val revenueData = List(("489434", "85048", "15CM CHRISTMAS GLASS BALL 20 LIGHTS", "12", "1/1/2009 7:45", "6.95", "13085", "United Kingdom", 83.4),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "12/31/2009 7:45", "6.75", "13085", "United Kingdom", 81.0),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "120", "12/1/2009 7:45", "1", "13085", "United Kingdom", 120.0),
      ("489434", "79323W", "WHITE CHERRY LIGHTS", "0", "12/1/2009 7:45", "6.75", "13085", "United Kingdom", 0.0),
      ("489434", "22041", "RECORD FRAME 7 SINGLE SIZE", "48", "12/1/2009 7:45", "2.1", "13085", "United Kingdom", 100.80000000000001),
      ("489434", "21232", "STRAWBERRY CERAMIC TRINKET BOX", "24", "12/1/2009 7:45", "1.25", "13085", "United Kingdom", 30.0),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", 39.599999999999994),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", 39.599999999999994),
      ("489434", "22", "PINK DOUGHNUT TRINKET POT", "24", "05/12/2009 7:45", "1.65", "13085", "United Kingdom", 39.599999999999994))

    val expectedDf = sanitizeData(revenueData.toDF("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country", "Revenue"))
    val revenueDF = addRevenue(sanitizeData(df))

    revenueDF.show(false)
    expectedDf.show(false)

    revenueDF.collect.toList should contain theSameElementsAs expectedDf.collect.toList
  }

  test("Should Add Product column in the dataset") {
    val productDF = addProductCode(sanitizeData(df))
    assert(productDF.columns.size === df.columns.size + 1)
  }

  test("Should get first 3 char of StockCode column as ProductCode column") {
    val productData = List(("489434", "85048", "15CM CHRISTMAS GLASS BALL 20 LIGHTS", "12", "1/1/2009 7:45", "6.95", "13085", "United Kingdom", "850"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "12/31/2009 7:45", "6.75", "13085", "United Kingdom", "793"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "120", "12/1/2009 7:45", "1", "13085", "United Kingdom", "793"),
      ("489434", "79323W", "WHITE CHERRY LIGHTS", "0", "12/1/2009 7:45", "6.75", "13085", "United Kingdom", "793"),
      ("489434", "22041", "RECORD FRAME 7 SINGLE SIZE", "48", "12/1/2009 7:45", "2.1", "13085", "United Kingdom", "220"),
      ("489434", "21232", "STRAWBERRY CERAMIC TRINKET BOX", "24", "12/1/2009 7:45", "1.25", "13085", "United Kingdom", "212"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", "220"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", "220"),
      ("489434", "22", "PINK DOUGHNUT TRINKET POT", "24", "05/12/2009 7:45", "1.65", "13085", "United Kingdom", "22"))

    val expectedDf = sanitizeData(productData.toDF("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country", "ProductCode"))

    val productDF = addProductCode(sanitizeData(df))

    productDF.show(false)
    expectedDf.show(false)

    productDF.collect.toList should contain theSameElementsAs expectedDf.collect.toList

  }

  test("Should add InvoiceMonth column in the dataset from invoice date") {
    val productData = List(("489434", "85048", "15CM CHRISTMAS GLASS BALL 20 LIGHTS", "12", "1/1/2009 7:45", "6.95", "13085", "United Kingdom", "2009-1"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "12/31/2009 7:45", "6.75", "13085", "United Kingdom", "2009-12"),
      ("489434", "79323P", "PINK CHERRY LIGHTS", "120", "12/1/2009 7:45", "1", "13085", "United Kingdom", "2009-12"),
      ("489434", "79323W", "WHITE CHERRY LIGHTS", "0", "12/1/2009 7:45", "6.75", "13085", "United Kingdom", "2009-12"),
      ("489434", "22041", "RECORD FRAME 7 SINGLE SIZE", "48", "12/1/2009 7:45", "2.1", "13085", "United Kingdom", "2009-12"),
      ("489434", "21232", "STRAWBERRY CERAMIC TRINKET BOX", "24", "12/1/2009 7:45", "1.25", "13085", "United Kingdom", "2009-12"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", "2009-12"),
      ("489434", "22064", "PINK DOUGHNUT TRINKET POT", "24", "12/1/2009 7:45", "1.65", "13085", "United Kingdom", "2009-12"),
      ("489434", "22", "PINK DOUGHNUT TRINKET POT", "24", "05/12/2009 7:45", "1.65", "13085", "United Kingdom", "2009-5"))

    val expectedDf = sanitizeData(productData.toDF("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country", "InvoiceMonth"))

    val dateMonthDF = addDateMonth(sanitizeData(df))

    dateMonthDF.show(false)
    expectedDf.show(false)

    dateMonthDF.collect.toList should contain theSameElementsAs expectedDf.collect.toList

  }

  test("Should calculate total revenue by Stock Code") {
    val revenueData = List(("79323W", 0.0),
      ("21232", 30.0),
      ("22", 39.599999999999994),
      ("22041", 100.80000000000001),
      ("22064", 79.19999999999999),
      ("85048", 83.4),
      ("79323P", 201.0))
    val expectedDf = revenueData.toDF("StockCode", "TotalRevenue")


    val totalRevenueByStockDF = calculateTotalRevenueByStockCode(addRevenue(sanitizeData(df)))

    totalRevenueByStockDF.show(false)
    expectedDf.show(false)

    totalRevenueByStockDF.collect.toList should contain theSameElementsAs expectedDf.collect.toList

  }

  test("Should calculate top 10 products by ProductCode") {
    val revenueData = List(("850", 83.4),
      ("793", 67.0),
      ("220", 60.0),
      ("22", 39.599999999999994),
      ("212", 30.0))
    val expectedDf = revenueData.toDF("ProductCode", "Top10Products")

    val top10Products = calTop10Products(addRevenue(addProductCode(sanitizeData(df))))

    top10Products.show(false)
    expectedDf.show(false)

    top10Products.collect.toList should contain theSameElementsAs expectedDf.collect.toList

  }

  test("Should calculate monthly revenue") {
    val revenueData = List(("2009-1", 83.4),
      ("2009-5", 39.599999999999994),
      ("2009-12", 411.0))
    val expectedDf = revenueData.toDF("InvoiceMonth", "TotalRevenue")

    val top10Products = calMonthlyRevenue(addRevenue(addDateMonth(sanitizeData(df))))

    top10Products.show(false)
    expectedDf.show(false)

    top10Products.collect.toList should contain theSameElementsAs expectedDf.collect.toList

  }


  //  df.show(false)

}
