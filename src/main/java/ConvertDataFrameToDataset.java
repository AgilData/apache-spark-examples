import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ConvertDataFrameToDataset {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("Example")
            .getOrCreate();

        Dataset<Row> df = spark.read()
            .option("header", "true")
            .csv("testdata/people.csv");

        Dataset<Person> ds = df.map(new MapFunction<Row, Person>() {
            @Override
            public Person call(Row value) throws Exception {
                return new Person(Integer.parseInt(value.getString(0)), value.getString(1), value.getString(2));
            }
        }, Encoders.bean(Person.class));


        ds.show();
    }



}

