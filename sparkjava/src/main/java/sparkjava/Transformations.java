package sparkjava;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Transformations {

    public static JavaRDD<Game> parseCsvToGame(JavaSparkContext jc, JavaRDD<String> lines) {
        return lines.map(i -> {
            try {
                String[] vals = i.split(",");
                return new Game(Integer.parseInt(vals[0]), vals[1], vals[5], vals[4]);
            } catch (Throwable cause) {
                // cause.printStackTrace();
                return null;
            }
        }).filter(i -> i != null);
    }

    public static JavaPairRDD<String, Long> gamesCountByPublishers(JavaSparkContext jc,
                                                                   JavaRDD<Game> games) {
        return games.keyBy(i -> i.getPublisher()).aggregateByKey(0l,
                (st, i) -> st + 1, (a, b) -> a + b);
    }

}
