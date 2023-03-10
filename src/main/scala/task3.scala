import org.apache.spark.sql.SparkSession
import java.io.Serializable
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.mllib.classification
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator



object task3 {

  val schema = new StructType(Array(
    StructField("exchange",DataTypes.StringType),
    StructField("stock_symbol",DataTypes.StringType),
    StructField("date",DataTypes.StringType),
    StructField("stock_price_open",DataTypes.DoubleType),
    StructField("stock_price_high",DataTypes.DoubleType),
    StructField("stock_price_low",DataTypes.DoubleType),
    StructField("stock_volume",DataTypes.DoubleType),
    StructField("label",DataTypes.IntegerType)
  ))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("task3")
      .getOrCreate()

    val stocksDF = spark.read
      .option("header", "true")
      .schema(schema)
      .format("csv")
      .load("input/stock_data.csv")

    val userSelectCols = List("exchange", "stock_symbol", "date")

    //feature engineering
    //???????????????????????????????????????????????????

    //Alpha#6: (-1 * correlation(open, volume, 10))
    //Alpha#23: (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)

    val indexers = userSelectCols.map(col => {
      new StringIndexer().setInputCol(col).setOutputCol(col + "Index")
    }).toArray

    val labelTransformed = new Pipeline().setStages(indexers).fit(stocksDF).transform(stocksDF).drop("exchange", "stock_symbol", "date").cache()

    // ??????????????????????????????
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("stock_price_open", "stock_price_high", "stock_price_low", "stock_volume"))
      //      .setInputCols(getColumnArray(rawInput))
      .setOutputCol("features")

    val xgbInput = vectorAssembler.transform(labelTransformed).select("features", "label").na.fill(0)

    xgbInput.show(10, false)

    //?????????????????????
    val Array(train, test) = xgbInput.randomSplit(Array(0.8, 0.2))

    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
      .setRegParam(0.1) //?????????
      .setElasticNetParam(0.3)
      .setMaxIter(50)

//    val model = lr.fit(train)
//
//    val predictions = model.transform(test)

    val model = new NaiveBayes().fit(train)

    val predictions = model.transform(test)

    predictions.show()

//    // ??????!!!??????num_workers ?????????????????? local[5] ?????????,?????????????????????????????????.
//    val xgbParam = Map("eta" -> 0.1f,
//      "max_depth" -> 2,
//      "objective" -> "binary:logistic",
////      "num_class" -> 2,
//      "num_round" -> 10,
//      "num_workers" -> 2)

//    // ??????xgboost??????,???????????????????????????
//    val xgbClassifier = new XGBoostClassifier(xgbParam)
//      .setFeaturesCol("features")
//      .setLabelCol("label")
//      .setMissing(0)
//
//    //????????????
//    val xgbClassificationModel = xgbClassifier.fit(train)
//    //??????
//    val predictions = xgbClassificationModel.transform(test)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    println(s"Test set accuracy = $accuracy")

    spark.close()
  }

}