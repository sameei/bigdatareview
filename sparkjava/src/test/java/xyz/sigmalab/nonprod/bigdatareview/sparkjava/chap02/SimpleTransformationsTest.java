package xyz.sigmalab.nonprod.bigdatareview.sparkjava.chap02;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class SimpleTransformationsTest {

    SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local");

    JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkContext(sparkConf));

    public static String file(String file) {
        return String.format("../dataset/%s", file);
    }

    @DisplayName("Test how first spark test works")
    @Test
    public void doSomething() {

        JavaRDD<String> lines =
                javaSparkContext.textFile(file("ce-pom.xml"));

        JavaRDD<String> words = SimpleTransformations.countWords(lines.rdd(),
                new HashSet<>(Arrays.asList("dependency", "dependencies"))).toJavaRDD();

        words.persist(StorageLevel.MEMORY_AND_DISK_SER());
        List<String> collected = words.collect();
        long countOfAll = words.count();

        System.out.printf("Words(%d): \n", countOfAll);
        for (String i: collected)
            System.out.println("\t " + i);

    }

}
