package processTime_eventTime_groupWindow;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
//todo 以处理时间  开10秒滚动窗口

public class ProcessTime_groupWindow_Tumble {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<NewSensorReading> mapDS = socketDS.map(new MapFunction<String, NewSensorReading>() {
            @Override
            public NewSensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //todo 每个窗口每个id的总数,并输出窗口的开始和结束时间

        //1.tablAPI 操作
        Table table = tableEnv.fromDataStream(mapDS,"id,ts,temp,pt.proctime");
        Table tableResult = table.window(
                Tumble.over("10.seconds").on("pt").as("wt"))
                .groupBy("id,wt")
                .select("id,id.count,wt.start");

        //2.sqlAPI操作
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id),tumble_end(pt,interval '10' second) from sensor" +
                //记得这里group by前加空格
                " group by id,tumble(pt,interval '10' second)");
        tableEnv.toRetractStream(tableResult, Row.class).print("table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sql");

        env.execute();
    }
}
