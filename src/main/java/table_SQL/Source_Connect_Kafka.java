package table_SQL;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
// todo 从kafka主题中消费数据,相当于source为kafka,但是读取的文件类型为csv或者json
public class Source_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置kafka连接器
        tableEnv.connect(new Kafka().version("0.11").topic("test").property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092").property(ConsumerConfig.GROUP_ID_CONFIG,"0720"))
                //定义输入文件格式
                //.withFormat(new Csv())
                .withFormat(new Json())
                .withSchema(new Schema().field("id",DataTypes.STRING()).field("ts",DataTypes.BIGINT()).field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");
        //SQLAPI可以直接查询
        Table sqlResult = tableEnv.sqlQuery("select id,max(temp) from kafka group by id");
        //tableAPI先创建表
        Table kafkaTable = tableEnv.from("kafka");
        Table max = kafkaTable.groupBy("id").select("id,temp.max");

        tableEnv.toRetractStream(max,Row.class).print("maxTable");
        tableEnv.toRetractStream(sqlResult,Row.class).print("maxSQL");
        env.execute();
    }
}
