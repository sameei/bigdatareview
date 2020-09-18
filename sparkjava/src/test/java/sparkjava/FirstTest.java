package sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FirstTest {

    @Test
    public void parseGameFromCSV() {

        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jc = new JavaSparkContext(sc);

        JavaRDD<String> jrdd =
                jc.textFile("/Volumes/Extra/Workspace/bigdatareview/dataset/VideoGamesSales.csv",
                        1).cache();

        JavaRDD<Game> games = Transformations.parseCsvToGame(jc, jrdd);

        long expected = jrdd.count() - 1;
        long result = games.count();

        System.out.printf("All: %d, Parsed: %d", expected, result);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void countByPublisher() {
        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jc = new JavaSparkContext(sc);

        JavaRDD<Game> games = jc.parallelize(Arrays.asList(
                new Game(1, "AAA", "EA", "Sport"),
                new Game(1, "BBB", "EA", "Sport"),
                new Game(1, "CCC", "EA", "Sport"),
                new Game(1, "DDD", "Cybercraft", "Sport")
        ));

        JavaPairRDD<String, Long> byPublisher =
                Transformations.gamesCountByPublishers(jc, games);

        Map<String, Long> counts = byPublisher.collectAsMap();

        System.out.println(counts);

        assertThat(counts).hasSize(2);
        assertThat(counts.get("EA")).isNotNull().isEqualTo(3);
        assertThat(counts.get("Cybercraft")).isNotNull().isEqualTo(1);
    }

}
