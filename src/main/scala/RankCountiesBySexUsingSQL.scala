import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexUsingSQL {

  def main(arg: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

    import sparkSession.implicits._

    var geoRDD = sparkSession.read.text("testdata/cogeo2010.sf1")
      .map(row => Geo(
        row.getString(0).substring(18,25), // Logical Record No
        row.getString(0).substring(226,316).trim, // Name
        row.getString(0).substring(8,11) // Summary Level (050 is county)
      ))

//    val geoDF: DataFrame = sparkSession.createDataFrame(geoRDD, StructType(List(
//      StructField("logrecid", DataTypes.StringType),
//      StructField("name", DataTypes.StringType),
//      StructField("sumlev", DataTypes.StringType)
//    ))).alias("geo")
//
//    val file18 = sparkSession.read.text("testdata/co000182010.sf1")
//      .map(row => Row(row.getString(0).split(","): _*))
//      .map(row => Row(row.getString(4), row.getString(6).toInt, row.getString(30).toInt, row.getString(6).toInt * 1.0f / row.getString(30).toInt))
//
//    val countyPopulation: DataFrame = sqlContext.createDataFrame(file18, StructType(List(
//      StructField("logrecid", DataTypes.StringType),
//      StructField("male", DataTypes.IntegerType),
//      StructField("female", DataTypes.IntegerType),
//      StructField("m2f", DataTypes.FloatType)
//    ))).alias("pop")
//
//    geoDF.registerTempTable("geo")
//    countyPopulation.registerTempTable("pop")
//
//    sqlContext.sql(
//      "SELECT geo.name, pop.male, pop.female, pop.m2f " +
//      "FROM geo JOIN pop ON geo.logrecid = pop.logrecid " +
//      "WHERE geo.sumlev = '050' " +
//      "ORDER BY m2f LIMIT 10"
//    ).collect().foreach(println)

  }

}





