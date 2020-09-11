package xyz.sigmalab.nonprod.bigdatareview.sparkjava.chap02;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.Set;

public class SimpleTransformations {

    public static final String SPLIT_REGEX = "(?:--|[ =<>\"'.\\/\\?!{}\\$,|:])";

    public static RDD<String> countWords(RDD<String> lines, Set<String> wordsToFilter) {

        JavaSparkContext jc = new JavaSparkContext(lines.context());
        JavaRDD<String> jlines = lines.toJavaRDD();

        LongAccumulator countOfLines =
                jc.sc().longAccumulator("Count of all lines");

        LongAccumulator countOfFilteredEmptyLines =
                jc.sc().longAccumulator("Count of filtered empty lines");

        LongAccumulator countOfWords =
                jc.sc().longAccumulator("Count of all words");

        LongAccumulator countOfFilteredWords =
                jc.sc().longAccumulator("Count of filtered words");

        Broadcast<Set<String>> brWordsToFilter =
                jc.broadcast(wordsToFilter);

        return jlines.filter(l -> {
            countOfLines.add(1);
            if (l.isEmpty()) {
                countOfFilteredEmptyLines.add(1);
                return false;
            }
            return true;
        }).flatMap(l ->
                Arrays.asList(l.split(SPLIT_REGEX)).iterator()
        ).filter(l -> {
            return !l.isEmpty();
        }).filter(i -> {
            countOfWords.add(1);
            boolean filtered = brWordsToFilter.value().contains(i);
            if (filtered) countOfFilteredWords.add(1);
            return !filtered;
        }).rdd();
    }
}
