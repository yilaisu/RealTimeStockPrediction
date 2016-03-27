/**
 *  * Craigslist example
 *
 * It predicts job category based on job description (called "job title").
 *
 * Launch following commands:
 *    export MASTER="local-cluster[3,2,4096]"
 *   bin/sparkling-shell -i examples/scripts/stockPrediction.scala
 *
 * When running using spark shell or using scala rest API:
 *    SQLContext is available as sqlContext
 *    SparkContext is available as sc
 */
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame

import _root_.hex.Distribution.Family
import _root_.hex.deeplearning.DeepLearningModel
import _root_.hex.tree.gbm.GBMModel
import _root_.hex.{Model, ModelMetricsBinomial}
import org.apache.spark.SparkFiles
import org.apache.spark.examples.h2o.DemoUtils.{addFiles, splitFrame}
import org.apache.spark.examples.h2o.{DemoUtils, Crime, RefineDateColumn}
import org.apache.spark.h2o._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import Array._
import sqlContext.implicits._

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

implicit val sqlContext = SQLContext.getOrCreate(sc)


/* --------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------  MODULE 2: FEATURE EXTRACTION  ---------------------------------------------------- 
--------------------------------------------------------------------------------------------------------------------------------- */

val data = sc.wholeTextFiles("news_articles/Apple/*/*.txt")

val fileName = data.map(l => l._1)
val content = data.map(l => l._2)

// All strings which are not useful for text-mining
val stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so","has","its")

// Define tokenizer function
def token(line: String): Array[String] = {
    //get rid of nonWords such as puncutation as opposed to splitting by just " "
    line.split("""\W+""") 
    .map(_.toLowerCase)
    //remove mix of words+numbers
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) 
    //remove stopwords defined above (you can add to this list if you want)
    .filterNot(word => stopwords.contains(word)) 
    //leave only words greater than 1 characters. 
    //this deletes A LOT of words but useful to reduce our feature-set
    .filter(word => word.size >= 2)  
}

def makeTwoWordComb(array: Array[String]): Array[String] = {

    for(i <- 0 until array.length if i < (array.length - 5) ){
      array(i) = 
        array(i) + " " + array(i+1) + "," +
        array(i) + " " + array(i+2) + "," +
        array(i) + " " + array(i+3) + "," +
        array(i) + " " + array(i+4) + "," +
        array(i) + " " + array(i+5)
    } 

    val arrayKeyVal = array.flatMap(line => line.split(","))
    .filter(word => word.contains(" "))

    return arrayKeyVal
}

val cleaned = content.map(d => token(d))
val twoWordComb = cleaned.map(d => (makeTwoWordComb(d)).toSeq )

val twc = cleaned.map(d => (makeTwoWordComb(d)))

val reduced = twoWordComb.reduce(_ union _)

val reducedRDD = sc.parallelize(reduced)
val reducedKeyVal = reducedRDD.map( word => (word,1)).reduceByKey(_+_)

val sortedFeatures = reducedKeyVal.map(item => item.swap).sortByKey(false, 1).map(item => item.swap)
// Now we have list of f 2-word comb features sorted by frequency of occurrence


/* --------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------  MODULE 3: FEATURE SELECTION  --------------------------------------------------- 
--------------------------------------------------------------------------------------------------------------------------------- */

val selectedFeatures = sortedFeatures.take(1000).map(d => d._1)
val dateColumn = Array("Year", "Month", "Day")
val stockColumn = Array("StockUp")
val selectedFeaturesWithDate = concat( dateColumn, selectedFeatures)
val intermediate = concat( selectedFeaturesWithDate, stockColumn)
val selectedFeaturesWithStockAndDate = concat(intermediate, dateColumn)


// Generate the schema based on the string of schema
val schema =
  StructType(
    selectedFeaturesWithDate.map(fieldName => StructField(fieldName, IntegerType, true)))

val schemaWithStockAndDateData = StructType(
    selectedFeaturesWithStockAndDate.map(fieldName => StructField(fieldName, IntegerType, true)))

def featureFilter(array: Array[String]): Array[String] = {
    array.filter(word => selectedFeatures.contains(word)) 
}

val filteredFeatures = twoWordComb.map(d => (featureFilter(d.toArray)).toSeq)


def peter(array: Array[String]): Array[Int] = {

    var arr = new Array[Int](selectedFeatures.length)

    for(i <- 0 until selectedFeatures.length ){
        
        var counter = 0

        for(j <- 0 until array.length ){
            if( selectedFeatures(i) == array(j) ){
                counter+=1
            }
        }
        arr(i) =  counter
    } 
    return arr
}

// filteredFeaturesRDD table dimensions: # articles x # features used
//      individual cell values are occurrence count of a particular feature in a particular article
val filteredFeaturesRDD = filteredFeatures.map( row => peter(row.toArray).toSeq )

def get_fileDate_from_fileName(fileName: String): Array[Int] = {

    val splitBySlash = fileName.split("/")
    val size = splitBySlash.length
    val date = splitBySlash(size-2)
    val fileDateString = date.split("-")
    val fileDateInt = fileDateString.map(_.toInt)

    return fileDateInt
}

val fileDate = fileName.map(filePath => get_fileDate_from_fileName(filePath).toSeq )


val filteredFeaturesWithDateRDD = fileDate.zip(filteredFeaturesRDD).map( v => concat(v._1.toArray, v._2.toArray).toSeq)
//val resultRDD: DataFrame = 
//val table:H2OFrame = resultRDD


def predict_by_article(year: String, month: String, day: String, model: Model[_,_,_])
    (implicit sqlContext: SQLContext, h2oContext: H2OContext): H2OFrame = {
    import h2oContext._
    import sqlContext.implicits._  

    // 
    //  FEATURE EXTRACTION
    // 

    // read in data
    val pDate = year + "-" + month + "-" + day
    val pFilePath = "news_articles/Apple/" + pDate  + "/*.txt"
    val predictionData = sc.wholeTextFiles(pFilePath)

    val pFileName = predictionData.map(l => l._1)
    val pContent = predictionData.map(l => l._2)

    // create two work comb
    val pCleaned = pContent.map(d => token(d))
    val pTwoWordComb = pCleaned.map(d => (makeTwoWordComb(d)).toSeq )
    val pReduced = pTwoWordComb.reduce(_ union _)

    val pReducedRDD = sc.parallelize(pReduced)
    val pReducedKeyVal = pReducedRDD.map( word => (word,1)).reduceByKey(_+_)
    // val pSortedFeatures = pReducedKeyVal.map(item => item.swap).sortByKey(false, 1).map(item => item.swap)



    // 
    val pFilteredFeatures = pTwoWordComb.map(d => (featureFilter(d.toArray)).toSeq)
    val pFilteredFeaturesRDD = pFilteredFeatures.map( row => peter(row.toArray).toSeq )

    val pFileDate = pFileName.map(pFilePath => get_fileDate_from_fileName(pFilePath).toSeq )


    val pFilteredFeaturesWithDateRDD = pFileDate.zip(pFilteredFeaturesRDD).map( v => concat(v._1.toArray, v._2.toArray).toSeq)

    val pRowRDD = pFilteredFeaturesWithDateRDD.map(p => Row.fromSeq(p))
    val pFeatureDataFrame = sqlContext.createDataFrame(pRowRDD, schema)
    pFeatureDataFrame.registerTempTable("pFeatureDate")

    val pRateFrame:H2OFrame = h2oContext.asH2OFrame(pFeatureDataFrame, Some("prediction_for_"+pDate))


      // Create a single row table
      // val srdd:DataFrame = sqlContext.sparkContext.parallelize(Seq(crime)).toDF()
      // // Join table with census data
      // val row: H2OFrame = censusTable.join(srdd).where('Community_Area === 'Community_Area_Number) //.printSchema
      //DemoUtils.allStringVecToCategorical(pRateFrame)
      val predictTable = model.score(pRateFrame)
      //val probOfArrest = predictTable.vec("true").at(0)
      predictTable  
      //println(probOfArrest.toFloat)
    //return 0
}


// // Start H2O services
import org.apache.spark.mllib
import org.apache.spark.h2o._
implicit val h2oContext = new H2OContext(sc).start()
import h2oContext._

val rowRDD = filteredFeaturesWithDateRDD.map(p => Row.fromSeq(p))
val featureDataFrame = sqlContext.createDataFrame(rowRDD, schema)
featureDataFrame.registerTempTable("featureDate")

val rateFrame:H2OFrame = h2oContext.asH2OFrame(featureDataFrame, Some("row_article_column_twoWordComb"))


/*
    NEXT UP IMPORT STOCK DATA
*/

//
// H2O Data loader using H2O API
//
def loadData(datafile: String): H2OFrame = new H2OFrame(new java.net.URI(datafile))

//
// Loader for feature table
//
def createWeatherTable(datafile: String): H2OFrame = {
  val table = loadData(datafile)
  // Remove first column since we do not need it
  //table.remove(0).remove()
  table.update(null)
  table
}

//
// Loader for stock data
//
def createCensusTable(datafile: String): H2OFrame = {
  val table = loadData(datafile)
  // Rename columns: replace ' ' by '_'
  val colNames = table.names().map( n => n.trim.replace(' ', '_').replace('+','_'))
  table._names = colNames
  //table.remove(8).remove()
  table.remove(7).remove()
  table.remove(6).remove()
  table.remove(5).remove()
  table.remove(4).remove()
  table.remove(3).remove()
  table.remove(2).remove()
  table.remove(1).remove()
  table.remove(0).remove()
  table.update(null)
  table
}

//
// Load data
//
addFiles(sc,
  "input.csv",
  "news_articles/apple_short.csv"
)

// 2-word comb features data
// val weatherTable = asDataFrame(createWeatherTable(SparkFiles.get("input.csv")))(sqlContext)
// weatherTable.registerTempTable("input")
// Census data
val censusTable = asDataFrame(createCensusTable(SparkFiles.get("apple_short.csv")))(sqlContext)

val toInt = udf[Int,Short](_.toInt)
val toIntB = udf[Int,Byte](_.toInt)
val censusTableNew = censusTable.withColumn("StockUp1", toIntB(censusTable("StockUp"))).withColumn("Year1", toInt(censusTable("Year"))).withColumn("Month1", toIntB(censusTable("Month"))).withColumn("Day1", toIntB(censusTable("Day"))).select("StockUp1","Year1","Month1","Day1")

censusTableNew.registerTempTable("stockData")


//
// Join crime data with weather and census tables
//
val stockAndFeatures = sqlContext.sql("SELECT * FROM featureDate a JOIN stockData b ON a.Year = b.Year1 AND a.Month = b.Month1 AND a.Day = b.Day1").collect//.stripMargin

var stockAndFeaturesRDD = sc.parallelize(stockAndFeatures)

//NOTE: print stockAndFeaturesRDD to csv file, and load into H2OFrame


val stockAndFeaturesDF = sqlContext.createDataFrame(stockAndFeaturesRDD, schemaWithStockAndDateData)
//val ab:H2OFrame = stockAndFeaturesDF
// val a :H2OFrame = stockAndFeatures
val stockAndFeaturesHF:H2OFrame = h2oContext.asH2OFrame(stockAndFeaturesDF, Some("row_article_column_twoWordComb_with_stockData"))

stockAndFeaturesHF.remove(1006).remove()
stockAndFeaturesHF.remove(1005).remove()
stockAndFeaturesHF.remove(1004).remove()
stockAndFeaturesHF.remove(2).remove()
stockAndFeaturesHF.remove(1).remove()
stockAndFeaturesHF.remove(0).remove()
stockAndFeaturesHF.update(null)

/* --------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------  MODULE 4: MODEL GENERATION  --------------------------------------------------- 
--------------------------------------------------------------------------------------------------------------------------------- */

val keys = Array[String]("train.hex", "test.hex")
val ratios = Array[Double](0.8, 0.2)
val frs = splitFrame(stockAndFeaturesHF, keys, ratios)
val (train, test) = (frs(0), frs(1))

def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
             ntrees:Int = 10, depth:Int = 6) //, distribution: Family = Family.bernoulli
            (implicit h2oContext: H2OContext) : GBMModel = {
  import h2oContext._
  import _root_.hex.tree.gbm.GBM
  import _root_.hex.tree.gbm.GBMModel.GBMParameters

  val gbmParams = new GBMParameters()
  gbmParams._train = train
  gbmParams._valid = test
  gbmParams._response_column = response
  gbmParams._ntrees = ntrees
  gbmParams._max_depth = depth
  //gbmParams._distribution = distribution

  val gbm = new GBM(gbmParams)
  val model = gbm.trainModel.get
  model
}

//
// Build GBM model
//
val gbmModel = GBMModel(train, test, "StockUp")

/* --------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------  PREDICTION FUNCTION  --------------------------------------------------- 
--------------------------------------------------------------------------------------------------------------------------------- */


// Transform all string columns into categorical
//DemoUtils.allStringVecToCategorical(stockAndFeaturesDF)

// stockAndFeatures.remove(1007).remove()
// stockAndFeatures.remove(1006).remove()
// stockAndFeatures.remove(1005).remove()
// stockAndFeatures.remove(2).remove()
// stockAndFeatures.remove(1).remove()
// stockAndFeatures.remove(0).remove()
// stockAndFeatures.update(null)

// // this is used to implicitly convert an RDD to a DataFrame.
// import sqlContext.implicits._

// // implicit call of H2OContext.asH2OFrame[Weather](rdd) is used 
// val hf: H2OFrame = sortedFeatures

openFlow



