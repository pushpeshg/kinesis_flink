package aws.finance;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KinesisConsumer {
    public static void main(String ... args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        consumerConfig.put(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "AKIAJKQFHCHYWIZN43HA");
        consumerConfig.put(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "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");
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
                "kinesis-analytics-demo-stream", new SimpleStringSchema(), consumerConfig));

        kinesis.print().setParallelism(1);

        env.execute("Reading from AWS Kinesis");
    }
}
