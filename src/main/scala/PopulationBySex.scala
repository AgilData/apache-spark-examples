import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object PopulationBySex {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // P12. SEX BY AGE [49]

    // (FILEID, STUSAB, CHARITER, CIFSN, and LOGRECNO) are first 5 fields in each data file

    // PCT13
    //SF1ST,CO,000,18,0034888,
    // 55607,
    // 27605,
    //       1998,2232,2004,1184, 619,324,295,1030,1911,2028
    //      ,2289,2224,2200,2056,1659,546,716, 376, 474, 586,395,277,182,
    // 28002,1946,2
    val file18: DataFrame = sqlContext.read.text("/home/andy/Documents/US_Census/2010/SF1/co000182010.sf1")

    val csv = file18.map(row => Row(row.getString(0).split(","): _*))

    csv
      .filter(row => row.getString(4) == "0034888") // find Broomfield County
      .collect().foreach(row => println(s"Total population: ${row.get(5)}; M: ${row.get(6)}; F: ${row.get(30)}"))

  }



}



