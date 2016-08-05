import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class JRankCountiesBySexUsingSQL {

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


//
//    Dataset<Row> dataFrame = sqlContext.createDataFrame(geoRDD, );

//    Encoder<JGeo> encoder = Encoders.bean(JGeo.class);
//    sqlContext.createDataset(geoRDD, encoder)
//
//    geoDF.printSchema();
//
//    DataFrame popDF = sqlContext.createDataFrame(populationRDD, JPopulation.class).alias("pop");
//    popDF.printSchema();
//
//    geoDF.registerTempTable("geo");
//    popDF.registerTempTable("pop");
//
//    //TODO: fails with: can not access a member of class JGeo with modifiers "public"
//    Row[] rows = sqlContext.sql(
//        "SELECT geo.name, pop.male, pop.female, pop.male/pop.female as m2f " +
//            "FROM geo JOIN pop ON geo.logrecno = pop.logrecno " +
//            "WHERE geo.sumlev = '050' " +
//            "ORDER BY m2f LIMIT 10"
//    ).collect();
//
//    Arrays.stream(rows).forEach(System.out::println);

  }
}
