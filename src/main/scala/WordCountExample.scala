import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WordCountExample {

  def main(arg: Array[String]): Unit = {

    FileUtils.deleteDirectory(new File("testdata/words_scala.txt"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

    import spark.implicits._

    val textFile: RDD[String] = spark.sparkContext.textFile("testdata/shakespeare.txt")

    textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 100)
      .sortBy(_._2, ascending = false)
      .saveAsTextFile("testdata/words_scala.txt")



    // if you want to try adding more transformations, here are some challenges:
    // - filter out empty strings
    // - filter out words with fewer than N characters
    // - convert all words to lower case before the map operation
    // - change the number of partitions
  }


}
