package etc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaSparkUtil {

    private final KafkaUtil ku;
    private final SparkSession spark;

    public KafkaSparkUtil(KafkaUtil ku, SparkSession spark) {
        this.ku = ku;
        this.spark = spark;
    }

    public KafkaUtil getKafkaUtil() {
        return ku;
    }

    public SparkSession getSparkSession() {
        return spark;
    }

    public Dataset<Row> readStream(String topic, String group, KafkaUtil.NoOffsetStrategy nofs) {

        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", ku.getBootstraperServer())
                .option("subscribe", topic)
                .option("startingOffsets", nofs.getV())
                .option("kafka.group.id", group)
                .load()
                .selectExpr(
                        "CAST(key as string) as key",
                        "CAST(value as string) as value",
                        "partition", "offset", "timestamp", "timestampType");

        /*
        06:42:04.825 WARN  org.apache.spark.sql.kafka010.KafkaSourceProvider -
        Kafka option 'kafka.group.id' has been set on this query, it is
        not recommended to set this option. This option is unsafe to use since multiple concurrent
        queries or sources using the same group id will interfere with each other as they are part
        of the same consumer group. Restarted queries may also suffer interference from the
        previous run having the same group id. The user should have only one query per group id,
        and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
        consumers from the previous query are marked dead by the Kafka group coordinator before the
        restarted query starts running.
        */
    }

    public List<Row> runAndReturn(Dataset<Row> df, Duration runFor) {

        StreamingQuery inq = null;
        try {
            String tblName = ku.newName("memorysink");

            inq = df.writeStream().format("memory")
                    .queryName(tblName)
                    .trigger(Trigger.ProcessingTime("0 seconds"))
                    .start();

            inq.awaitTermination(runFor.toMillis());
            inq.stop();

            return spark.sql("SELECT * FROM " + tblName).collectAsList();

        } catch (Exception cause) {
            throw new RuntimeException(cause);
        } finally {
            if (inq != null) try {
                inq.stop();
            } catch (Exception cause) {
                throw new RuntimeException(cause);
            }
        }
    }

    public <R> List<R> runAndMap(Dataset<Row> df, Duration runFor, Function<Row, R> mapf) {
        return runAndReturn(df, runFor).stream().map(mapf).collect(Collectors.toList());
    }

    public void toConsole(Dataset<Row> df, Duration maxWait) {
        StreamingQuery inq = null;
        try {
            inq = df.writeStream()
                    .format("console")
                    .outputMode(OutputMode.Update())
                    .trigger(Trigger.ProcessingTime("0 seconds"))
                    .option("truncate", false)
                    .option("numRows", 50)
                    .start();
            inq.awaitTermination(maxWait.toMillis());
        } catch (Exception cause) {
            throw new RuntimeException(cause);
        } finally {
            if (inq != null) try {
                inq.stop();
            } catch (Exception cause) {
                throw new RuntimeException(cause);
            }
        }
    }

}
