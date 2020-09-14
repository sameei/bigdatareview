package xyz.sigmalab.nonprod.bigdatareview.sparkjava.chap02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Util {

    public static String file(String file) {
        return String.format("../dataset/%s", file);
    }

    public static Dataset<Row> fromCSV(SparkSession spark, String file) {
        return spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(Util.file(file));
    }

}
