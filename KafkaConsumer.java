import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.8.238:9092");
        properties.setProperty("group.id", "flink_consumer");

        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test",  new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        ((DataStream<String>)env.addSource(kafkaConsumer).setParallelism(1))
                .flatMap((String value, Collector<WordWithCount> tmp) -> {
                            for (String word : value.split("\\s")) {
                                tmp.collect(new WordWithCount(word, 1L));
                            }
                        }
                ).returns(WordWithCount.class).setParallelism(1)
                .keyBy("word")
                .timeWindow(Time.seconds(1))
                .reduce((w1,w2)->new WordWithCount(w1.word, w1.count + w2.count)).setParallelism(1)
                .print().setParallelism(1);

        env.execute("Kafka Window WordCount");
    }

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}

