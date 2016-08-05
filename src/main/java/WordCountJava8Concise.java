import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class WordCountJava8Concise {

  public static void main(String[] args) throws Exception {

    FileUtils.deleteDirectory(new File("testdata/words_java.txt"));

    SparkConf sparkConf = new SparkConf()
        .setAppName("Example")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // open the text file as an RDD of String
    sc.textFile("testdata/shakespeare.txt")
        .flatMap(line -> Arrays.asList(WordHelper.split(line)).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .filter(tuple -> tuple._2() > 100)
        .saveAsTextFile("testdata/words_java.txt");

  }

}
