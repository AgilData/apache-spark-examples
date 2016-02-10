import org.apache.spark.{SparkContext, SparkConf}

object WordCountExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textFile = sc.textFile("testdata/shakespeare.txt")

    textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 100)
      .sortBy(_._2, ascending = false)
      .saveAsTextFile("testdata/words_scala.txt")
  }

}
