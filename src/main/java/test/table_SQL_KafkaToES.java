package test;

import bean.Word;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
//todo 读取kafka数据写入ES,计算wordcount
//输入数据如下:
//hello,atguigu,hello
//hello,spark
//hello,flink
public class table_SQL_KafkaToES {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //消费kafka的sensor主题的数据
        DataStreamSource sensor = env.addSource(new FlinkKafkaConsumer011("sensor", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator WordDS = sensor.flatMap(new FlatMapFunction<String, Word>() {
            @Override
            public void flatMap(String value, Collector<Word> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(new Word(word));
                }
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("wordcount",WordDS);
        Table SQlResult = tableEnv.sqlQuery("select word,count(*) ct from wordcount group by word");
        tableEnv.connect(
                new Elasticsearch()
                        .version("6")
                        .host("hadoop102",9200,"http")
                        .index("wordcount")
                        .documentType("_doc")
                        //执行一条显示一条
                        .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("ct",DataTypes.BIGINT()))
                //更新模式,必须指定模式
                .inUpsertMode()
                .createTemporaryTable("wc");
        tableEnv.toRetractStream(SQlResult, Row.class).print();
        tableEnv.insertInto("wc",SQlResult);
        env.execute();
    }
}
