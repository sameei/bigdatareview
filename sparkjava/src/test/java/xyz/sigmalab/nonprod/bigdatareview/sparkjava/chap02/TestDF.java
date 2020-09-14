package xyz.sigmalab.nonprod.bigdatareview.sparkjava.chap02;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;

public class TestDF {

  SparkSession sparkSession =
      SparkSession.builder().appName("Sample").master("local").getOrCreate();

  public class Person {
    private final String name;
    private final int age;

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public int getAge() {
      return age;
    }
  }

  @Test
  public void fromPOJO() {
    RowFactory.create("Reza", 32);

    Dataset<Row> people =
        sparkSession.createDataFrame(
            Arrays.asList(
                new Person("Reza", 32),
                new Person("Mohsen", 35),
                new Person("Majid", 31),
                new Person("Hamid", 34),
                new Person("Moein", 28),
                new Person("Reza", 31),
                new Person("Tom Cruise", 58)),
            Person.class);

    people.show();
  }

  @Test
  public void fromRow() {

    List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("Name", DataTypes.StringType, false, Metadata.empty()),
            DataTypes.createStructField("Age", DataTypes.IntegerType, false, Metadata.empty()));
    StructType schema = DataTypes.createStructType(fields);

    JavaRDD<Row> rdd =
        new JavaSparkContext(sparkSession.sparkContext())
            .parallelize(
                Arrays.asList(
                    RowFactory.create("Reza", 32),
                    RowFactory.create("Tom Cruise", 58),
                    RowFactory.create("Mohsen", 35),
                    RowFactory.create("Mohammad", 34),
                    RowFactory.create("MohammadAmin", 34)));

    Dataset<Row> ds = sparkSession.createDataFrame(rdd, schema);

    ds.show();
  }

  @Test
  public void fromRange() {
    Dataset<Row> numbers = sparkSession.range(1, 10).toDF("number");
    Dataset<Row> ds01 =
        numbers
            .select(numbers.col("number").plus(10).as("col_a"))
            .select("col_a")
            .withColumn("col_b", col("col_a").multiply(-1))
            .withColumn("col_e", lit("Hello dudes"))
        // .selectExpr("col_a", "col_b", "\"Hallo Leüte\"")
        ;
    Dataset<Row> ds02 =
        sparkSession.range(11, 20, 2).toDF("col_c").selectExpr("col_c", "col_c - 7 as col_d");

    ds01.join(ds02, ds01.col("col_a").equalTo(ds02.col("col_c"))).show();
  }

  @Test
  public void dfWordCount() {
    Dataset<Row> df = Util.fromCSV(sparkSession, "SherLock/T2.csv");
    df.printSchema();
    // df.show(10);
    // df.repartition(1);
    df.selectExpr("UserId", "UUID", "Version", "TimeStemp").createOrReplaceTempView("T2");
    sparkSession.sql("SELECT * FROM T2").show(10);
  }

  @Test
  public void dfJavaSerialization() {
    Dataset<Row> df =
        Util.fromCSV(sparkSession, "SherLock/T2.csv")
            .select("UserId", "UUID", "Version", "TimeStemp");

    df.printSchema();
    StructType schema = df.schema();

    df.javaRDD().saveAsObjectFile(Util.file("as-javaobject"));

    JavaRDD rows = new JavaSparkContext(sparkSession.sparkContext()).objectFile(Util.file("temp"));
    sparkSession.createDataFrame(rows, schema).show(10);
  }

  @Test
  public void dfParquet() {
    Dataset<Row> df =
        sparkSession
            .read()
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(Util.file("SherLock/T2.csv"));

    df.coalesce(1).write().parquet(Util.file("as-parquet"));

    sparkSession.read().parquet(Util.file("as-parquet")).show(10);
  }

  @Test
  public void dfUDF() {
    sparkSession.udf().register("my_inc", new MyInc(), DataTypes.LongType);
    sparkSession
        .range(1, 10)
        .toDF("number")
        .selectExpr("my_inc(number) as x")
        .where("x % 2 = 0")
        .show();
  }

  public static class MyInc implements UDF1<Long, Long> {
    @Override
    public Long call(Long num) throws Exception {
      if (num == null) return null;
      return num + 1;
    }
  }
}
