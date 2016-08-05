import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function2;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

  public static void main(String[] args) throws Exception {

    FileUtils.deleteDirectory(new File("testdata/words_java.txt"));

    SparkConf sparkConf = new SparkConf()
        .setAppName("Example")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // open the text file as an RDD of String
    JavaRDD<String> textFile = sc.textFile("testdata/shakespeare.txt");

    // convert each line into a collection of words
    JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(WordHelper.split(s)).iterator();
      }
    });

    // map each word to a tuple containing the word and the value 1
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String word) { return new Tuple2<>(word, 1); }
    });

    // for all tuples that have the same key (word), perform an aggregation to add the counts
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
      }
    });

    // perform some final transformations, and then save the output to a file
    counts.filter(tuple -> tuple._2() > 100)
            .saveAsTextFile("testdata/words_java.txt");

  }

}
