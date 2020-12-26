package etc;

import org.junit.Test;

public class KafkaSparkTest {

    @Test
    public void firstCasesWithErliest() {
        // Publish and read 9 messages
        KafkaSparkCases.firstCase("fce", "127.0.0.1:9092", KafkaUtil.NoOffsetStrategy.EARLIEST);
    }

    @Test
    public void firstCasesWithLatest() {
        // Publish 9 messages, but read 0
        KafkaSparkCases.firstCase("fcl", "127.0.0.1:9092", KafkaUtil.NoOffsetStrategy.LATEST);
    }

}
