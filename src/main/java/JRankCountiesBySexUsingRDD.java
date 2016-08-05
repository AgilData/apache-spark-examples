import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class JRankCountiesBySexUsingRDD {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<JGeo> geoRDD = sc.textFile("testdata/cogeo2010.sf1")
            .map(s -> new JGeo(
                s.substring(18,25), // Logical Record No
                s.substring(226,316).trim(), // Name
                s.substring(8,11) // Summary Level (050 is county)
        ));

        JavaRDD<JPopulation> populationRDD = sc.textFile("testdata/co000182010.sf1")
                .map(s -> s.split(","))
                .map(row -> new JPopulation(row[4], Integer.parseInt(row[6]), Integer.parseInt(row[30])));

        // to do a join, we first need to make paired RDDs
        JavaPairRDD<String, JGeo> geoPairRDD = geoRDD.keyBy(JGeo::getLogrecno);
        JavaPairRDD<String, JPopulation> populationPairRDD = populationRDD.keyBy(JPopulation::getLogrecno);

        // now for the join
        JavaPairRDD<String, Tuple2<JGeo, JPopulation>> joined = geoPairRDD.join(populationPairRDD);

        // now we want to merge the pairs of tuples back down into a simpler structure
        JavaRDD<JPopulationSummary> popSummary = joined.map(x -> new JPopulationSummary(
            x._1(),
            x._2()._2().getMale(),
            x._2()._2().getFemale()
        ));

        popSummary
            .sortBy((Function<JPopulationSummary, Object>) p -> p.getMale() * 1.0f / p.getFemale(), true, 1)
            .take(10)
            .forEach(System.out::println);
    }
}

