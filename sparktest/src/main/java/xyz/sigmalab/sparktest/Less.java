package xyz.sigmalab.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Less {

    // $SPARK_HOME/bin/spark-submit \
    //      --class xyz.sigmalab.sparktest.Less \
    //      --master spark://192.168.1.114:7077 \
    //      ./target/sparktest-1.0-SNAPSHOT.jar \
    //      hdfs:///path-to-file/file.txt hdfs:///path-for-result

    public static void main(String[] args) {

        String src = args[0];
        System.out.println("SRC: " + src);
        String dest = args[1];
        System.out.println("DESC: "+ dest);

        SparkConf conf = new SparkConf().setAppName("Less");
        JavaSparkContext jctx = new JavaSparkContext(conf);
        JavaRDD<String> jlines = jctx.textFile(src);
        jlines.saveAsTextFile(dest);

    }

}
