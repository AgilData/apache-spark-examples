import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexUsingDataFrame {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    var geoRDD = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/cogeo2010.sf1")
      .map(row => Row(
        row.getString(0).substring(18,25), // Logical Record No
        row.getString(0).substring(226,316).trim, // Name
        row.getString(0).substring(8,11) // Summary Level (050 is county)
      ))

    val geoDF: DataFrame = sqlContext.createDataFrame(geoRDD, StructType(List(
      StructField("logrecid", DataTypes.StringType),
      StructField("name", DataTypes.StringType),
      StructField("sumlev", DataTypes.StringType)
    ))).alias("geo")

    val file18 = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/co000182010.sf1")
      .map(row => Row(row.getString(0).split(","): _*))
      .map(row => Row(row.getString(4), row.getString(6).toInt, row.getString(30).toInt, row.getString(6).toInt * 1.0f / row.getString(30).toInt))

    val countyPopulation: DataFrame = sqlContext.createDataFrame(file18, StructType(List(
      StructField("logrecid", DataTypes.StringType),
      StructField("male", DataTypes.IntegerType),
      StructField("female", DataTypes.IntegerType),
      StructField("m2f", DataTypes.FloatType)
    ))).alias("pop")

    geoDF.printSchema()
    geoDF.limit(5).collect().foreach(println(_))

    countyPopulation.printSchema()
    countyPopulation.limit(5).collect().foreach(println(_))

    geoDF.filter(geoDF.col("logrecid").equalTo(lit("0034888"))).collect().foreach(println(_))
    countyPopulation.filter(countyPopulation.col("logrecid").equalTo(lit("0034888"))).collect().foreach(println(_))

    val join = geoDF
      .filter(geoDF.col("sumlev").equalTo(lit("050")))
      .join(countyPopulation, "logrecid")

    join.printSchema()


    println(s"Counties with lowest ratio of Male to Female:")
    join
      .orderBy(countyPopulation.col("m2f"))
      .limit(10)
      .collect()
      .foreach(row => println(s"${row.getString(1)} M: ${row.getInt(3)}; F: ${row.getInt(4)}; Ratio: ${row.getFloat(5)}"))

    println(s"Counties with highest ratio of Male to Female:")
    join
      .orderBy(countyPopulation.col("m2f").multiply(lit(-1.0f)))
      .limit(10)
      .collect()
      .foreach(row => println(s"${row.getString(1)} M: ${row.getInt(3)}; F: ${row.getInt(4)}; Ratio: ${row.getFloat(5)}"))


  }

}





