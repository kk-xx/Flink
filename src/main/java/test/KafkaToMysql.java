package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
//todo 消费kafka的数据,传入mysql
public class KafkaToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        //读取kafka中数据
        DataStreamSource kafkaDS = env.addSource(new FlinkKafkaConsumer011("sensor", new SimpleStringSchema(), properties));
        //将数据wordcount
        SingleOutputStreamOperator kwords = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        SingleOutputStreamOperator sum = kwords.keyBy(0).sum(1);
        sum.addSink(new MySQL());
        env.execute();
    }

    private static class MySQL extends RichSinkFunction<Tuple2<String, Integer>> {
        //声明JDBC连接
        private Connection connection;

        //声明预编译SQL
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement("INSERT INTO sensor_id_count(id,count) VALUES(?,?) ON DUPLICATE KEY UPDATE count=?;");

        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            preparedStatement.setString(1, value.f0);
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setInt(3, value.f1);

            //执行
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
