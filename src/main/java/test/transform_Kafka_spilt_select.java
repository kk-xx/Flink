package test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import source.SensorReading;

import java.util.Collections;
import java.util.Properties;

//todo 消费kafka的数据,按照温度30度,进入两个流
public class transform_Kafka_spilt_select {
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
        SingleOutputStreamOperator SensorDs = sensor.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] words = value.split(",");
                return new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2]));
            }
        });
        SplitStream split = sensor.split(new OutputSelector<SensorReading>() {

            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getV() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream high = split.select("high");
        DataStream low = split.select("low");
        high.print();
        low.print();
        env.execute();
    }
}
