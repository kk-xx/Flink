package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

//todo 将数据流的数据写入kafka的tokafka1主题
public class Sink_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<NewSensorReading> sensorDS = env.socketTextStream("hadoop102", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                }
        );
        //写tableAPI得写fromDataStream,然后对着结果table写逻辑
        Table table = tableEnv.fromDataStream(sensorDS);
        Table tableResult = table.select("id,temp");
        //写sqlAPI得用createTemporaryView给流创建视图,创建表名
        tableEnv.createTemporaryView("sensor",sensorDS);
        Table SQLResult = tableEnv.sqlQuery("select id,temp from sensor");
        //创建连接器,得到临时表
        tableEnv.connect(new Kafka().version("0.11").topic("tokafka1").property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                //定义输出文件格式
                //.withFormat(new Csv())
                .withFormat(new Json())
                //字段属性,个数一定要与输出的时候一样
                .withSchema(new Schema().field("id", DataTypes.STRING()).field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaout");
        //将查询结果写入临时表
        tableEnv.insertInto("kafkaout",tableResult);
        tableEnv.insertInto("kafkaout",SQLResult);

        env.execute();
    }
}
