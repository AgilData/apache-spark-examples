import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
        .setAppName("Example")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> textFile = sc.textFile("testdata/shakespeare.txt");
    JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
    });
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) { return new Tuple2<>(s, 1); }
    });
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b)
        .filter(tuple -> tuple._2() > 100);
    counts.saveAsTextFile("testdata/words_java.txt");

  }

}
