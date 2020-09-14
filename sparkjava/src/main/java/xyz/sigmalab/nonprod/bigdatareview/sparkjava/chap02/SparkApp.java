package xyz.sigmalab.nonprod.bigdatareview.sparkjava.chap02;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Collections;

public class SparkApp {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jc = new JavaSparkContext(sc);

        JavaRDD<String> jrdd =
                jc.textFile(
                        "/Volumes/Extra/Workspace/bigdatareview/dataset/SherLock/SMS.csv", 1);

        JavaRDD<String> rdd2 = jrdd.map( i -> i.toLowerCase());

        rdd2.persist(StorageLevel.MEMORY_ONLY());

        rdd2.flatMap(i -> {
            if (i.isEmpty()) return Collections.emptyIterator();
            return Arrays.asList(i.split(",")).iterator();
        });

        rdd2.coalesce(1);
        rdd2.repartition(10);
        rdd2.distinct(3);

        rdd2.map(i -> i.toLowerCase());
        rdd2.mapPartitions( iter -> {
            // conect to redis
            while(iter.hasNext()) {
                iter.next();
            }
            return Collections.<Integer>emptyIterator();
        });

        JavaPairRDD<Character, String> prdd = rdd2.keyBy(i -> i.charAt(0));

        int result = prdd.aggregate(0, (st, v) -> {
            return st + 1;
        }, (i1, i2) -> {
            return i1 + i2;
        });

        JavaPairRDD<Character, Integer> x = prdd.aggregateByKey(0, (st, str) -> {
            return st + 1;
        }, (i1, i2) -> {
            return i1 + i2;
        });

        doNothing(rdd2, "Hello").take(10).forEach( i -> System.out.println(i));
        rdd2.take(10).forEach(i -> System.out.println(i));
        System.out.println(rdd2.count());
    }

    public static JavaRDD<String> doNothing(JavaRDD<String> rdd, String filter) {
        return rdd.filter(i -> i.startsWith(filter));
    }

}
