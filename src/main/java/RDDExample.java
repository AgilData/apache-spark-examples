import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class RDDExample {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<JGeo> geoRDD = sc.textFile("/home/andy/Documents/US_Census/2010/SF1/cogeo2010.sf1")
            .map(s -> new JGeo(
                s.substring(18,25), // Logical Record No
                s.substring(226,316).trim(), // Name
                s.substring(8,11) // Summary Level (050 is county)
        ));

        JavaRDD<JPopulation> populationRDD = sc.textFile("/home/andy/Documents/US_Census/2010/SF1/co000182010.sf1")
                .map(s -> s.split(","))
                .map(row -> new JPopulation(row[4], Integer.parseInt(row[6]), Integer.parseInt(row[30])));

        // to do a join, we first need to make paired RDDs
        JavaPairRDD<String, JGeo> geoPairRDD = geoRDD.keyBy(JGeo::getLogrecno);
        JavaPairRDD<String, JPopulation> populationPairRDD = populationRDD.keyBy(JPopulation::getLogrecno);

        // now for the join
        JavaPairRDD<String, Tuple2<JGeo, JPopulation>> joined = geoPairRDD.join(populationPairRDD);

        // now we want to merge the pairs of tuples back down into a simpler structure
        JavaRDD<Object> flatRDD = joined.map(x -> new JPopulationSummary(
                x._1(),
                x._2()._2().male,
                x._2()._2().female
        ));

        // now we can sort, and grab the top N entries
        flatRDD.sortBy((Function<Object, Object>) obj -> {
            final JPopulationSummary p = (JPopulationSummary) obj;
            return p.male * 1.0f / p.female;
        }, true, 1)
                .top(10)
                .forEach(System.out::println);
    }
}

class JGeo implements Serializable {

    String logrecno;
    String name;
    String sumlev;

    public JGeo() {
    }

    public JGeo(String logrecno, String name, String sumlev) {
        this.logrecno = logrecno;
        this.name = name;
        this.sumlev = sumlev;
    }

    public String getLogrecno() {
        return logrecno;
    }

    public void setLogrecno(String logrecno) {
        this.logrecno = logrecno;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSumlev() {
        return sumlev;
    }

    public void setSumlev(String sumlev) {
        this.sumlev = sumlev;
    }

    @Override
    public String toString() {
        return "JGeo{" +
                "logrecno='" + logrecno + '\'' +
                ", name='" + name + '\'' +
                ", sumlev='" + sumlev + '\'' +
                '}';
    }
}

class JPopulation implements Serializable {

    String logrecno;
    int male;
    int female;

    public JPopulation(String logrecno, int male, int female) {
        this.logrecno = logrecno;
        this.male = male;
        this.female = female;
    }

    public String getLogrecno() {
        return logrecno;
    }

    public void setLogrecno(String logrecno) {
        this.logrecno = logrecno;
    }

    public int getMale() {
        return male;
    }

    public void setMale(int male) {
        this.male = male;
    }

    public int getFemale() {
        return female;
    }

    public void setFemale(int female) {
        this.female = female;
    }

    @Override
    public String toString() {
        return "Population{" +
                "logrecno='" + logrecno + '\'' +
                ", male=" + male +
                ", female=" + female +
                '}';
    }
}

class JPopulationSummary implements Serializable {

    String name;
    int male;
    int female;

    public JPopulationSummary() {
    }

    public JPopulationSummary(String name, int male, int female) {
        this.name = name;
        this.male = male;
        this.female = female;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMale() {
        return male;
    }

    public void setMale(int male) {
        this.male = male;
    }

    public int getFemale() {
        return female;
    }

    public void setFemale(int female) {
        this.female = female;
    }

    public float getMFRatio() {
        return male*1.0f/female;
    }

    @Override
    public String toString() {
        return "PopulationSummary{" +
                "name='" + name + '\'' +
                ", male=" + male +
                ", female=" + female +
                '}';
    }
}