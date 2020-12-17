package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//todo 从流中数据转换成样例类来查询,查询时字段名为样例类字段名
public class Source_Stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取tableAPI的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //读端口数据转换为javaBean
        SingleOutputStreamOperator<NewSensorReading> sensorDS = socketDS.map(line -> {
            String[] split = line.split(",");
            return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });
        //表环境流对数据流创建临时视图,声明表名与数据流
        tableEnv.createTemporaryView("sensor",sensorDS);
        //SQL方式实现查询
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id='sensor_1'");
        //tableAPI实现查询
        Table table = tableEnv.fromDataStream(sensorDS);
        Table tableResult = table.select("id,temp").where("id='sensor_1'");
        //输出结果
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");
        tableEnv.toRetractStream(tableResult,Row.class).print("table");
        //执行
        env.execute();

    }
}
