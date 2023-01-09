import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object task1 {

  val schema = new StructType(Array(
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
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("task1")
      .getOrCreate()

    val stocksDF = spark.read
      .option("header", "true")
      .schema(schema)
      .format("csv")
      .load("input/stock_small.csv")

    stocksDF.createOrReplaceTempView("dict")
    val dictDF1 = spark.sql("SELECT YEAR(date) AS Year,stock_symbol,SUM(stock_volume) AS Volume FROM dict GROUP BY stock_symbol,YEAR(date) " +
      "ORDER BY Volume DESC")

    val dictDF2 = spark.sql("SELECT exchange,stock_symbol,date,stock_price_close,stock_price_open, stock_price_close-stock_price_open AS differ" +
      " FROM dict ORDER BY differ DESC limit 10")

    dictDF1.show(10)
    dictDF2.show()
    //    stocksDF.show(10)
  }

}