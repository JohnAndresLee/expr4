import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object task2 {

  val schema1 = new StructType(Array(
    StructField("exchange",DataTypes.StringType),
    StructField("stock_symbol",DataTypes.StringType),
    StructField("date",DataTypes.DateType),
    StructField("stock_price_open",DataTypes.DoubleType),
    StructField("stock_price_high",DataTypes.DoubleType),
    StructField("stock_price_low",DataTypes.DoubleType),
    StructField("stock_price_close",DataTypes.DoubleType),
    StructField("stock_volume",DataTypes.DoubleType),
    StructField("stock_price_adj_close",DataTypes.DoubleType)
  ))

  val schema2 = new StructType(Array(
    StructField("exchange", DataTypes.StringType),
    StructField("symbol", DataTypes.StringType),
    StructField("date", DataTypes.DateType),
    StructField("dividend", DataTypes.DoubleType)
  ))


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("task2")
      .getOrCreate()

    val stocksDF = spark.read
      .option("header", "true")
      .schema(schema1)
      .format("csv")
      .load("input/stock_small.csv")

    val dividendsDF = spark.read
      .option("header", "true")
      .schema(schema2)
      .format("csv")
      .load("input/dividends_small.csv")


    stocksDF.createOrReplaceTempView("stocks")
    dividendsDF.createOrReplaceTempView("dividends")

    val dictDF1 = spark.sql("SELECT date,stock_symbol,stock_price_close FROM stocks WHERE stock_symbol='IBM' AND " +
      "date IN (SELECT date FROM dividends WHERE symbol='IBM' AND YEAR(date)>2000)")

    val dictDF2 = spark.sql("SELECT YEAR(date), MEAN(stock_price_adj_close) AS year_stock_price_adj_close FROM stocks " +
      "WHERE stock_symbol='AAPL' GROUP BY YEAR(DATE) HAVING year_stock_price_adj_close>50")

    dictDF1.show()
    dictDF2.show()
  }

}