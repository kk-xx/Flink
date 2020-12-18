package processTime_eventTime_groupWindow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

//todo 根据DDL语句获得事件时间
public class EventTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.获得tableAPI的执行环境
        EnvironmentSettings bsSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //2.创建tableAPI执行环境
        StreamTableEnvironment bstableEnv = StreamTableEnvironment.create(env, bsSetting);

        //3.建表时创建时间字段为事件时间
        String singkDDL="CREATE TABLE sensor ( id varchar(20),ts bigint,temp double," +
                //设置事件时间
              "rt as to_timestamp(from_unixtime(ts)),watermark for rt as rt-interval '1' second"+
                ")WITH(" +
                "  'connector.type' = 'filesystem'," +
                "  'connector.path' = 'input/sensor.txt', " +
                "  'format.type' = 'csv')";
        bstableEnv.sqlUpdate(singkDDL);
        //4.读取数据创建表
        Table sensor = bstableEnv.from("sensor");
        //5输出表信息
        sensor.printSchema();
        //root
        // |-- id: VARCHAR(20)
        // |-- ts: BIGINT
        // |-- temp: DOUBLE
        // |-- rt: TIMESTAMP(3) AS TO_TIMESTAMP(FROM_UNIXTIME(`ts`))
        // |-- WATERMARK FOR rt AS `rt` - INTERVAL '1' SECOND


        //        String singkDDL="CREATE TABLE sensor ( id varchar(20),temp double," +
        //                //设置事件时间
        //                "ts TIMESTAMP(3)," +
        //                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
        //                ")WITH(" +
        //                "  'connector.type' = 'filesystem'," +
        //                "  'connector.path' = 'input/sensor.txt', " +
        //                "  'format.type' = 'csv')";

        //root
        // |-- id: VARCHAR(20)
        // |-- temp: DOUBLE
        // |-- ts: TIMESTAMP(3)
        // |-- WATERMARK FOR ts AS `ts` - INTERVAL '5' SECOND
    }
}
