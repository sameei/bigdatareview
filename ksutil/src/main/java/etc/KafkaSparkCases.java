package etc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class KafkaSparkCases {

    public static <T> void printSeq(Iterable<T> iterable) {
        for(T t : iterable) {
            System.out.printf("ITEM : %s\n", t);
        }
    }

    public static void firstCase(String name, String srv, KafkaUtil.NoOffsetStrategy nofs) {

        KafkaUtil ku = new KafkaUtil(name, srv);
        String topicName = ku.newName("topic");
        String groupId = ku.newName("groupid");

        System.out.printf("Create topic '%s' ... \n", topicName);
        ku.createTopic(topicName);

        System.out.println("Publising Messages to the topic ...");
        ku.genAndPublish(topicName, "Message-%05d", 1, 10, 1);

        System.out.println("Checking the offset of the group ...");
        printSeq(ku.getOffsetsOfGroup(groupId).entrySet());

        SparkSession spark =
                SparkSession.builder()
                        .appName(ku.newName("spark"))
                        .master("local[*]")
                        .getOrCreate();

        KafkaSparkUtil ksu = new KafkaSparkUtil(ku, spark);

        Dataset<Row> df = ksu.readStream(topicName, groupId, nofs);
        // ksu.toConsole(df, java.time.Duration.ofSeconds(5));

        List<String> result =
                ksu.runAndMap(df, java.time.Duration.ofSeconds(5), r -> r.getString(1));
        printSeq(result);

        System.out.println("Checking the offset of the group, again, ...");
        printSeq(ksu.getKafkaUtil().getOffsetsOfGroup(groupId).entrySet());

        System.out.println("END");
    }

}
