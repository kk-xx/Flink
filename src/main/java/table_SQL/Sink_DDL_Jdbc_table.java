package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Sink_DDL_Jdbc_table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //对流中数据进行样例类处理
        SingleOutputStreamOperator<NewSensorReading> mapDS = socketDS.map(new MapFunction<String, NewSensorReading>() {
            @Override
            public NewSensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //对样例类处理过的的流注册
        Table table = tableEnv.fromDataStream(mapDS);
        //对表进行查询
        Table tableResult = table.select("id,temp");
        //配置连接的DDL
        //todo 注意:这里的建表语句必须和mysql已经建的表的建表语句一样
        String sinkDDL = "create table JdbcTable(id varchar(20) not null,temp double not null)"+"with"
                +"('connector.type'='jdbc'" +
                "," +
                "'connector.url'='jdbc:mysql://hadoop102:3306/test'" +
                "," +
                "'connector.table'='sensor_id'" +
                "," +
                "'connector.driver'='com.mysql.jdbc.Driver','connector.username'='root'" +
                "," +
                "'connector.password'='123456','connector.write.flush.max-rows'='1')"
                ;
        tableEnv.sqlUpdate(sinkDDL);
        //将流的数据插入mysql表
        tableResult.insertInto("JdbcTable");
        env.execute();
    }
}
