import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class ConvertCollectionToDataset {

  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Example")
        .getOrCreate();


    Encoder<JGeo> personEncoder = Encoders.bean(JGeo.class);
    Dataset<JGeo> javaBeanDS = spark.createDataset(
        Collections.singletonList(new JGeo("a", "b", "c")),
        personEncoder
    );
    javaBeanDS.show();

  }
}
