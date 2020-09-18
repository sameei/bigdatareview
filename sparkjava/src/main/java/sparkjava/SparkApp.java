package sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkApp {

    public static void main(String[] args) {
        byDataFrame();
    }

    public static void byRDD() {
        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jc = new JavaSparkContext(sc);

        JavaRDD<String> jrdd =
                jc.textFile("/Volumes/Extra/Workspace/bigdatareview/dataset/VideoGamesSales.csv",
                        1);

        JavaRDD<Game> games = Transformations.parseCsvToGame(jc, jrdd);
        JavaPairRDD<String, Long> byPublishers = Transformations.gamesCountByPublishers(jc, games);
        byPublishers.foreach(t -> System.out.println(t));
    }

    public static void byDataFrame() {
        SparkSession spark =
                SparkSession.builder().appName("Sample").master("local").getOrCreate();

        Dataset<Row> ds =
                spark.read()
                        .format("csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load("/Volumes/Extra/Workspace/bigdatareview/dataset/VideoGamesSales.csv");

        // ds.show();
        // ds.printSchema();

        ds.col("Name");
        functions.col("Name");

        ds = ds.select("Rank", "Name", "Publisher", "Genre")
                .groupBy("Publisher", "Genre")
                .agg(functions.count("Name").as("Games"))
                .sort("Publisher", "Games", "Genre");
        // ds.show();
        ds.groupBy("Publisher").agg(functions.count("Genre")).show(Integer.MAX_VALUE, false);
    }

}
