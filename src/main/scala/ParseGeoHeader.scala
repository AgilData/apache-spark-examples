import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{UserDefinedFunction, Row, DataFrame, SQLContext}

object ParseGeoHeader {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //val parseGeo: UserDefinedFunction = sqlContext.udf.register("parseGeo", (s: String) => s.split(" "))

    var geo: DataFrame = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/cogeo2010.sf1")

    //geo = geo.filter(geo.col("value").contains("80020"))

    // parse by mapping to RDD[Row]
    val map: RDD[Row] = geo.map(row => Row(parseGeo(row.getString(0))))
      .filter(row => row.get(0).asInstanceOf[Geo].sumlev == "050")
      .filter(row => row.get(0).asInstanceOf[Geo].name contains "Broomfield")

    map.take(5).foreach(println(_))



    // parse by invoking UDF
//    geo = geo.select(parseGeo.apply(geo.col("value")))

//    geo.printSchema()
//    geo.show(5, false)



  }

  def parseGeo(s: String) = {
    Geo(
      s.substring(18,25),
      s.substring(226,316).trim,
      s.substring(318,327),
      s.substring(8,11)
    )
  }

}

case class Geo(logrecno: String, name: String, population: String, sumlev: String)

