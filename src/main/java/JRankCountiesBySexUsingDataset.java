import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

public class JRankCountiesBySexUsingDataset {

  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Example")
        .getOrCreate();


    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    SQLContext sqlContext  = new SQLContext(sc);

    Dataset<JGeo> geo = sqlContext.read().text("testdata/cogeo2010.sf1")
        .map(new MapFunction<Row, JGeo>() {
               @Override
               public JGeo call(Row row) throws Exception {
                 return new JGeo(
                     row.getString(0).substring(18, 25), // Logical Record No
                     row.getString(0).substring(226, 316).trim(), // Name
                     row.getString(0).substring(8, 11) // Summary Level (050 is county)
                 );
               }
             }, Encoders.bean(JGeo.class));

    Dataset<JPopulation> pop = sqlContext.read().text("testdata/co000182010.sf1")
        .map(new MapFunction<Row, JPopulation>() {
          @Override
          public JPopulation call(Row row) throws Exception {
            String[] split = row.getString(0).split(",");
            return new JPopulation(split[4], Integer.parseInt(split[6]), Integer.parseInt(split[30]));
          }
        }, Encoders.bean(JPopulation.class));

    Dataset<Row> join = geo.join(pop, "logrecno");

    // I'd like to do this but I can't use the scala lit function in Java
    // join.sort(join.col("male").multiply(lit(1.0f)).divide(join.col("female")));

    Dataset<Row> newDF = join.sort(join.col("male").divide(join.col("female")).alias("m2f"));
    newDF.show();

//    Row[] rows = newDF
//        .sort("m2f")
//        .limit(10)
//        .collect();
//
//    Arrays.stream(rows).forEach(System.out::println);
//

  }
}
