import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexRDD {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    var geoRDD = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/cogeo2010.sf1")
      .map(row => Geo(
        row.getString(0).substring(18,25), // Logical Record No
        row.getString(0).substring(226,316).trim, // Name
        row.getString(0).substring(8,11) // Summary Level (050 is county)
      )).filter(geo => geo.sumlev == "050")

    val populationRDD = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/co000182010.sf1")
      .map(row => row.getString(0).split(","))
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
    flatRDD.sortBy((p: CountyPopulation) => (p.male*1.0f/p.female), ascending = true)
      .zipWithIndex()
      .filter((t: (CountyPopulation, Long)) => (t._2 < 10))
      .foreach(println)

  }

}

case class Geo(logrecno: String, name: String, sumlev: String)

case class Population(logrecno: String, male: Int, female: Int)

case class CountyPopulation(county: String, male: Int, female: Int)




