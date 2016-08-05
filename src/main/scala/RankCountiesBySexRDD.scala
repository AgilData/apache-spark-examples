import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexRDD {

  def main(arg: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

    import sparkSession.implicits._

    var geoRDD: RDD[Geo] = sparkSession.sparkContext.textFile("testdata/cogeo2010.sf1")
      .map(row => Geo(
        row.substring(18,25), // Logical Record No
        row.substring(226,316).trim, // Name
        row.substring(8,11) // Summary Level (050 is county)
      )).filter(geo => geo.sumlev == "050")

    val populationRDD: RDD[Population] = sparkSession.sparkContext.textFile("testdata/co000182010.sf1")
      .map(row => row.split(","))
      .map(csv => Population(csv(4), csv(6).toInt, csv(30).toInt))

    // create pair RDDs
    val geoPair: RDD[(String, Geo)] = geoRDD.map(geo => (geo.logrecno, geo))
    val popPair: RDD[(String, Population)] = populationRDD.map(p => (p.logrecno, p))

    // join
    val join: RDD[(String, (Geo, Population))] = geoPair.join(popPair)

    // flatten
    val flatRDD: RDD[CountyPopulation] = join.map((tuple: (String, (Geo, Population)))
        => CountyPopulation(tuple._2._1.name, tuple._2._2.male, tuple._2._2.female))

    // show top N
    flatRDD.sortBy((p: CountyPopulation) => p.male*1.0f/p.female, ascending = true)
      .zipWithIndex()
      .filter((t: (CountyPopulation, Long)) => t._2 < 10)
      .foreach(println)

  }

}

case class Geo(logrecno: String, name: String, sumlev: String)

case class Population(logrecno: String, male: Int, female: Int)

case class CountyPopulation(county: String, male: Int, female: Int)




