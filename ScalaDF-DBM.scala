import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame

def loadTableDF(spark: SparkSession, path: String) = {
  spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(path)
}

def cleanTableDF(df: org.apache.spark.sql.DataFrame) = {
  val colName = df.columns(0) // Assuming there's only one column in the original CSV
  df.withColumn("key", split(col(colName), "\t").getItem(0).cast(IntegerType))
    .withColumn("value", split(col(colName), "\t").getItem(1).cast(IntegerType))
    .drop(colName)
}

val spark = SparkSession.builder()
  .appName("Data Loader and Cleaner")
  .getOrCreate()

// Assuming SparkSession is initialized as 'spark'
import spark.implicits._

// Load the DataFrame
val df = loadTableDF(spark, "/FileStore/tables/web_Google-4.txt")

// Clean the DataFrame
val cleanedDF = cleanTableDF(df)

// Show some rows to verify
cleanedDF.show()

def fccgIterate(df: DataFrame): (DataFrame, Long) = {
  // MAP
  val mapDF = df.union(df.select($"value".alias("key"), $"key".alias("value")))

  // REDUCE
  // Use collect set for deduplication !
  val dfAgg = mapDF.groupBy($"key").agg(
    collect_set($"value").alias("adj_list"),
    array_min(collect_set($"value")).alias("min_value")
  )

  val dfAggFiltered = dfAgg.filter($"key" > $"min_value")

  val newCount = dfAggFiltered.select(sum(size($"adj_list"))).first().getLong(0) - dfAggFiltered.count()

  // Concat the 'key' column with the 'adj_list' one into one array
  val dfConcat = dfAggFiltered.select(concat(array(col("key")), col("adj_list")).alias("adj_list"), col("min_value"))

  // Explode the 'adj_list' array to separate rows and include 'min_value' for comparison
  val dfExploded = dfConcat.select(
    explode(col("adj_list")).alias("key"),
    col("min_value").alias("value")
  )

  // Deduplicate the data
  val dfFinal = dfExploded.distinct()

  (dfFinal, newCount)
  }

def computeFCCG(df: DataFrame): DataFrame = {
    var nbIteration: Int = 0
    var loopDF: DataFrame = df
    var count: Long = 1

    while (count != 0) {
      nbIteration += 1
      
      val resultTuple: (DataFrame, Long) = fccgIterate_fast(loopDF)
      loopDF = resultTuple._1
      count = resultTuple._2

      println(s"Number of new pairs for iteration #$nbIteration:\t$count")
    }

    println("\nNo new pair, end of computation")
    return loopDF // Exit the loop and return the DataFrame
  }

val final_df = computeFCCG(cleanedDF)

// Find total number of connected components
final_df.select($"value").distinct().count()
