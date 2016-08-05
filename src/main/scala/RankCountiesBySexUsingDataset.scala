import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexUsingDataset {

  def main(arg: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

//    var geoRDD = sqlContext.read.text("testdata/cogeo2010.sf1")
//      .map(row => Geo(
//      row.getString(0).substring(18,25), // Logical Record No
//      row.getString(0).substring(226,316).trim, // Name
//      row.getString(0).substring(8,11) // Summary Level (050 is county)
//    )).filter(geo => geo.sumlev == "050")
//
//    val populationRDD = sqlContext.read.text("testdata/co000182010.sf1")
//      .map(row => row.getString(0).split(","))
//      .map(csv => Population(csv(4), csv(6).toInt, csv(30).toInt))
//
//    import sqlContext.implicits._
//
//    val geoDS: Dataset[Geo] = sqlContext.createDataset(geoRDD).as("geo")
//
//    val popDS: Dataset[Population] = sqlContext.createDataset(populationRDD).as("pop")
//
//    val join: Dataset[(Geo, Population)] = geoDS.joinWith(popDS, expr("geo.logrecno = pop.logrecno"))
//
//    join.printSchema()
//
//    // example of map with compile-time type safety
//    join.map((tuple: (Geo, Population)) => tuple._2.male > 10000)

    // oops, they forgot to add any sort operations
    //join.sortBy()



  }

}





