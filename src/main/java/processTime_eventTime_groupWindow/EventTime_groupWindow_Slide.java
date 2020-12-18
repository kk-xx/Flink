package processTime_eventTime_groupWindow;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
//todo 以处理时间  开10秒划动窗口

public class EventTime_groupWindow_Slide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<NewSensorReading> mapDS = socketDS.map(new MapFunction<String, NewSensorReading>() {
            @Override
            public NewSensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //设置watermark字段
        SingleOutputStreamOperator<NewSensorReading> watermarkDS = mapDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewSensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(NewSensorReading element) {
                return element.getTs() * 1000L;
            }
        });
        //todo 每个窗口每个id的总数,并输出窗口的开始和结束时间

        //1.tablAPI 操作,设置时间为事件时间
        Table table = tableEnv.fromDataStream(watermarkDS,"id,ts,temp,rt.rowtime");

        Table tableResult = table.window(
                Slide.over("5.seconds").every("2.seconds").on("rt").as("wt"))
                .groupBy("id,wt")
                .select("id,id.count,wt.start");

        //2.sqlAPI操作,设置时间为事件时间
        tableEnv.createTemporaryView("sensor",table);
        //滑动窗口的第一个为滑动步长,第二个为窗口大小
        Table sqlResult = tableEnv.sqlQuery("select id,count(id),hop_end(rt,interval '2' second,interval '5' second) from sensor" +
                //记得这里group by前加空格
                " group by id,hop(rt,interval '2' second,interval '5' second)");
        tableEnv.toRetractStream(tableResult, Row.class).print("table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sql");

        env.execute();
    }
}
