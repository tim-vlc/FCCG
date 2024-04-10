import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

val conf = new SparkConf().setAppName("FCCG RDD Implementation")
val sc = SparkContext.getOrCreate(conf)

def loadTableRDD(path: String): RDD[String] = {
  val rdd = sc.textFile(path)
  val header = rdd.first() // Extract header
  rdd.filter(row => row != header) // Filter out the header
}

// Load data into an RDD
var rdd = loadTableRDD("/FileStore/tables/web_NotreDame.txt")

def cleanTableRDD(rdd: RDD[String]): RDD[(Int, Int)] = {
  rdd.map(_.split("\t")).map(kv => (kv(0).toInt, kv(1).toInt))
}

var clean_rdd = cleanTableRDD(rdd)

def fccgIterate(rdd: RDD[(Int, Int)]): (RDD[(Int, Int)], Double) = {
  // MAP
  val rddMapped = rdd.union(rdd.map(kv => (kv._2, kv._1)))

  // REDUCE
  // Create adjacency list and find minimum value in each list
  val rddReduced = rddMapped.groupByKey().mapValues(values => {
    val valuesSet = values.toSet
    (valuesSet, valuesSet.min)
  })

  // Filter the rows that have min_value lower than key
  val rddFiltered = rddReduced.filter(kv => kv._1 > kv._2._2)

  // Calculate the new pairs created
  val newCount = rddFiltered.map(kv => kv._2._1.size - 1).sum()

  // Prepare for next iteration
  val rddFinal = rddFiltered.flatMap(kv => (kv._2._1 + kv._1).map(k => (k, kv._2._2))).distinct()

  (rddFinal, newCount)
}

def computeFccg(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
  var nbIteration = 0
  var loop = true
  var new_rdd = rdd

  while (loop) {
    nbIteration += 1

    val result = fccgIterate(new_rdd)
    val count = result._2
    new_rdd = result._1

    println(s"Number of new pairs for iteration #$nbIteration:\t$count")
    if (count == 0) {
      println("\nNo new pair, end of computation")
      loop = false
    }
  }
  new_rdd
}

var final_rdd = computeFccg(clean_rdd)

// Find the total number of connected components in graph
final_rdd.map(_._2).distinct().count()
