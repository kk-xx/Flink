package sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
//将hadoop102的9999端口数据发送到kafka的flinkSink主题中
public class ToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDs = env.socketTextStream("hadoop102", 9999);
        //设置kafka生产者的配置
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        //addsink,发送数据
        socketDs.addSink(new FlinkKafkaProducer011<String>("flinkSink",new SimpleStringSchema(),properties));
        //执行
        env.execute();
    }
}
