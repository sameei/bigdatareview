package xyz.sigmalab.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Less {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Less");
        JavaSparkContext jctx = new JavaSparkContext(conf);
        JavaRDD<String> jlines = jctx.textFile("file:///tmp/viminfo");
        jlines.saveAsTextFile("/tmp/viminfo-newone");

    }

}
