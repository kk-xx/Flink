package processTime_eventTime_groupWindow;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

//todo 根据流获取事件时间
public class EventTime_Stream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.设置eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.从流中获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //4.对流中数据进行样例类处理
        SingleOutputStreamOperator<NewSensorReading> mapDS = socketDS.map(new MapFunction<String, NewSensorReading>() {
            @Override
            public NewSensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //5.设置事件时间字段
        SingleOutputStreamOperator<NewSensorReading> eventTimeDS = mapDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewSensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(NewSensorReading element) {
                return element.getTs() * 1000L;
            }
        });
        //6.将流转换为表,并指定时间字段为处理时间,pt为随便起的名字
        Table table = tableEnv.fromDataStream(eventTimeDS, "id,ts,temp,et.rowtime");
        //7.打印表信息
        table.printSchema();
        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- temp: DOUBLE
        // |-- et: TIMESTAMP(3) *ROWTIME*
    }
}
