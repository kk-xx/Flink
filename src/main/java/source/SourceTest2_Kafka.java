package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
//todo 消费kafka的数据
public class SourceTest2_Kafka {
    public static void main(String[] args) throws Exception {
        // kafka配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //消费kafka的sensor主题的数据
        DataStreamSource sensor = env.addSource(new FlinkKafkaConsumer011("sensor", new SimpleStringSchema(), properties));
        sensor.print();
        env.execute();

    }


}
