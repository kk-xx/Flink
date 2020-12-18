package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Sink_DDL_Jdbc_sql_Upsert {
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
        tableEnv.createTemporaryView("sensor",mapDS);
        //对表进行查询
        Table SQLResult =  tableEnv.sqlQuery("select id,count(*) ctt from sensor group by id");
        //配置连接的DDL
        String sinkDDL = "create table JdbcTable(id varchar(20) not null,ct bigint not null)"+"with"
                +"('connector.type'='jdbc'" +
                "," +
                "'connector.url'='jdbc:mysql://hadoop102:3306/test'" +
                "," +
                "'connector.table'='a'" +
                "," +
                "'connector.driver'='com.mysql.jdbc.Driver','connector.username'='root'" +
                "," +
                "'connector.password'='123456','connector.write.flush.max-rows'='1')"
                ;
        tableEnv.sqlUpdate(sinkDDL);
        //将流的数据插入mysql表
        SQLResult.insertInto("JdbcTable");
        env.execute();
    }
}
