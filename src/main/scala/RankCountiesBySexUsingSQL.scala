import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object RankCountiesBySexUsingSQL {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

    import spark.implicits._

    var geo: Dataset[Geo] = spark.read.text("testdata/cogeo2010.sf1")
      .map(row => Geo(
        row.getString(0).substring(18,25), // Logical Record No
        row.getString(0).substring(226,316).trim, // Name
        row.getString(0).substring(8,11) // Summary Level (050 is county)
      )).alias("geo")
      .filter(geo => geo.sumlev == "050")

    val pop: Dataset[Population] = spark.read.text("testdata/co000182010.sf1")
      .map(row => row.getString(0).split(","))
      .map(csv => Population(csv(4), csv(6).toInt, csv(30).toInt))
      .alias("pop")


    geo.registerTempTable("geo")
    pop.registerTempTable("pop")

    spark.sql(
      "SELECT geo.name, pop.male, pop.female, pop.male/pop.female as m2f " +
      "FROM geo JOIN pop ON geo.logrecno = pop.logrecno " +
      "WHERE geo.sumlev = '050' " +
      "ORDER BY m2f LIMIT 10"
    ).collect().foreach(println)

  }

}





