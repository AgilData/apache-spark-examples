import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class JRankCountiesBySexUsingDataFrame {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
        .setAppName("Example")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<JGeo> geoRDD = sc.textFile("testdata/cogeo2010.sf1")
        .map(s -> new JGeo(
            s.substring(18,25), // Logical Record No
            s.substring(226,316).trim(), // Name
            s.substring(8,11) // Summary Level (050 is county)
        ));

    JavaRDD<JPopulation> populationRDD = sc.textFile("testdata/co000182010.sf1")
        .map(s -> s.split(","))
        .map(row -> new JPopulation(row[4], Integer.parseInt(row[6]), Integer.parseInt(row[30])));

//    Dataset[Row] geoDF = sqlContext.createDataFrame(geoRDD, JGeo.class).alias("geo");
//    geoDF.printSchema();
//
//    DataFrame popDF = sqlContext.createDataFrame(populationRDD, JPopulation.class).alias("pop");
//    popDF.printSchema();
//
//    DataFrame join = geoDF.join(popDF, "logrecno");
//
//    // I'd like to do this but I can't use the scala lit function in Java
//    //join.sort(join.col("male").multiply(lit(1.0f)).divide(join.col("female")));
//
//    DataFrame newDF = join.sort(join.col("male").divide(join.col("female")).alias("m2f"));
//
//    Row[] result = newDF
//        .sort("m2f")
//        .limit(10)
//        .collect();
//
//    Arrays.stream(result).forEach(System.out::println);


  }
}
