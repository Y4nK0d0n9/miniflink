import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.8.238:9092");
        properties.setProperty("group.id", "flink_consumer");

        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test",  new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> sourceStream = env.addSource(kafkaConsumer);

        // parse the data, group it, window it, and aggregate the counts
        DataStream<SocketWindowWordCount.WordWithCount> windowCounts = sourceStream

                .flatMap(new FlatMapFunction<String, SocketWindowWordCount.WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<SocketWindowWordCount.WordWithCount> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new SocketWindowWordCount.WordWithCount(word, 1L));
                        }
                    }
                })

                .keyBy("word")
                .timeWindow(Time.seconds(10))

                .reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
                    @Override
                    public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount a, SocketWindowWordCount.WordWithCount b) {
                        return new SocketWindowWordCount.WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);
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

