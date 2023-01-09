# expr4
### 任务一（基于Spark SQL）

**stock_small.csv文件schema的设置**

```scala
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
```

**读取stock_small.csv中的数据并创建视图表**

```scala
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
```

![image-20230106234222777](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/image-20230106234222777.png)

```scala
val dictDF1 = spark.sql("SELECT YEAR(date) AS Year,stock_symbol,SUM(stock_volume) AS Volume FROM dict GROUP BY stock_symbol,YEAR(date) ORDER BY Volume DESC")
```

前10行结果展示如下：

![Screenshot from 2023-01-09 08-51-43](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2008-51-43.png)

若要求输出每年的排序情况，则对Year属性列进行筛选即可

![image-20230106234436834](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/image-20230106234436834.png)

![image-20230106234456230](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/image-20230106234456230.png)

```scala
val dictDF2 = spark.sql("SELECT exchange,stock_symbol,date,stock_price_close,stock_price_open, stock_price_close-stock_price_open AS differ FROM dict ORDER BY differ DESC limit 10")
```

![Screenshot from 2023-01-09 08-53-04](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2008-53-04.png)

### 任务一（基于Spark）

**根据文件路径读取数据**

```scala
val conf = new SparkConf().setMaster("local").setAppName("Task1_Demo")
val sc = new SparkContext(conf)
val line = sc.textFile(inputFile)
```

**问题一的解决**


```scala
val data1 = line.map(x => (x.split(",")(2).substring(0,4) + x.split(",")(1), x.split(",")(7).toString.toInt)).reduceByKey(_ + _)

val count1 = data1.map(tuple=>(tuple._2,tuple._1)).sortByKey(ascending = false).map(tuple=>(tuple._2,tuple._1))

val result1 = count1.partitionBy(new CustomPartitioner(12)).
map(tuple=>"Year:"+ tuple._1.substring(0,4) + "stock_symbol:" + tuple._1.substring(4)+"Volume:"+tuple._2.toString)
```

**问题二的解决**

```scala
val data2 = line.map(x => (x.split(",")(0) + "," + x.split(",")(1) + "," + x.split(",")(2) + ","
+ x.split(",")(3) + "," + x.split(",")(6), (x.split(",")(6).toFloat - x.split(",")(3).toFloat).abs))

val count2 = data2.map(tuple=>(tuple._2,tuple._1)).sortByKey(ascending = false).take(10).map(tuple=>(tuple._2,tuple._1))

val result2 = count2.map(tuple=>"exchange:"+ tuple._1.split(",")(0) + "stock_symbol:" + tuple._1.split(",")(1) +
"date:" + tuple._1.split(",")(2) + ",stock_price_close" + tuple._1.split(",")(4) + ",stock_price_open" + tuple._1.split(",")(3) +
",differ:" + tuple._2.toString)
```

### 任务二（基于Spark SQL）

**dividends_small.csv文件schema的设置**

```scala
  val schema2 = new StructType(Array(
    StructField("exchange", DataTypes.StringType),
    StructField("symbol", DataTypes.StringType),
    StructField("date", DataTypes.DateType),
    StructField("dividend", DataTypes.DoubleType)
  ))
```

![image-20230106234333860](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/image-20230106234333860.png)

```scala
    val dictDF1 = spark.sql("SELECT date,stock_symbol,stock_price_close FROM stocks WHERE stock_symbol='IBM' AND date IN (SELECT date FROM dividends WHERE symbol='IBM' AND YEAR(date)>2000)")
```

![Screenshot from 2023-01-09 08-54-36](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2008-54-36.png)

![image-20230106234402307](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/image-20230106234402307.png)

```scala
    val dictDF2 = spark.sql("SELECT YEAR(date), MEAN(stock_price_adj_close) AS year_stock_price_adj_close FROM stocks WHERE stock_symbol='AAPL' GROUP BY YEAR(DATE) HAVING year_stock_price_adj_close>50")
```

![Screenshot from 2023-01-09 08-54-53](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2008-54-53.png)

### 任务二（基于Hive）

**表的创建与数据读取**

- stock_small表的创建

```hive
create table if not exists stock_small
(
    'exchange' string,
    'symbol' string,
    'ymd' string,
    'price_open' float,
    'price_high' float,
    'price_low' float,
    'price_close' float,
    'volume' int,
    'price_adj_close' float
)
row format delimited fields terminated by ',';
```

- dividends_small表的创建

```hive
create external table if not exists dividends_small
(
    'ymd' string,
    'dividend' float
)
partitioned by('exchange' string ,'symbol' string)
row format delimited fields terminated by ','
```

- 数据的读取

```hive
load data local inpath '/home/hadoop/expr4/stock_small.csv' overwrite into table stock_small;
load data local inpath '/home/hadoop/expr4/dividends_small.csv' overwrite into table dividends_small;
```

**问题一的解决**

```hive
SELECT s.ymd,s.symbol,s.price_close
FROM stock_small s 
LEFT SEMI JOIN 
dividends_small d
ON s.ymd=d.ymd and s.symbol=d.symbol
where s.symbol='IBM' and year(ymd)>=2000;
```

**问题二的解决**

```hive
select year(ymd) as year, avg(price_adj_close) as avg_price from stocks
where exchange='NASDAQ' and symbol='AAPL'
group by year(ymd)
having avg_price > 50;
```

### 任务三

索引与特征属性列的划分与转换

```scala
    val userSelectCols = List("exchange", "stock_symbol", "date")

    val indexers = userSelectCols.map(col => {
      new StringIndexer().setInputCol(col).setOutputCol(col + "Index")
    }).toArray

    val labelTransformed = new Pipeline().setStages(indexers).fit(stocksDF).transform(stocksDF).drop("exchange", "stock_symbol", "date").cache()
```

将所有特征列转成向量

```scala
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("stock_price_open", "stock_price_high", "stock_price_low", "stock_volume"))
      //      .setInputCols(getColumnArray(rawInput))
      .setOutputCol("features")

    val xgbInput = vectorAssembler.transform(labelTransformed).select("features", "label").na.fill(0)
```

特征和标签展示如下所示：

![Screenshot from 2023-01-09 15-31-11](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2015-31-11.png)

按照0.8和0.2的比例划分训练集与测试集

```scala
//训练集，预测集
val Array(train, test) = xgbInput.randomSplit(Array(0.8, 0.2))
```

- 逻辑斯蒂回归的模型设置与训练

```scala
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
      .setRegParam(0.1) //正则化
      .setElasticNetParam(0.3)
      .setMaxIter(50)

    val model = lr.fit(train)
    val predictions = model.transform(test)
```

计算得到回归的精度为：

```scala
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)

println(s"Test set accuracy = $accuracy")
```

![Screenshot from 2023-01-09 15-32-19](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2015-32-19.png)

- 贝叶斯模型的模型设置与训练

```scala
val model = new NaiveBayes().fit(train)

val predictions = model.transform(test)
```

计算得到回归的精度为：

![](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/Screenshot%20from%202023-01-09%2015-33-51.png)

- XGBoost的模型设置与训练

```scala
   val xgbParam = Map("eta" -> 0.1f,
     "max_depth" -> 2,
     "objective" -> "binary:logistic",
     "num_round" -> 10,
     "num_workers" -> 2)

   // 创建xgboost函数,指定特征向量和标签
   val xgbClassifier = new XGBoostClassifier(xgbParam)
     .setFeaturesCol("features")
     .setLabelCol("label")
     .setMissing(0)

   //开始训练
   val xgbClassificationModel = xgbClassifier.fit(train)
   //预测
   val predictions = xgbClassificationModel.transform(test)
```

### 可改进之处

- **开盘、最高、最低的时间序列上的分析**

当股票截面数据更全时，我们可以引入一些因子，进一步提取股票特征（以alpha191中因子为例）：

Alpha#6: (-1 * correlation(open, volume, 10))

Alpha#23: (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)

- **spark/scala版本兼容问题**

在运行程序时，经常遇到各种模块版本不兼容导致的奇怪报错，可以针对版本问题开发一个用户手册，方便以后使用。
