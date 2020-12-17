package watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
//todo 允许迟到数据
public class TumblingEventTime_Lateness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //设置eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<String> boundWatermark = socketDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = boundWatermark.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new Tuple2<>(split[0], 1));
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = tupleDS.keyBy(0);
        //开窗时,允许迟到数据
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windows = keyDS.timeWindow(Time.seconds(5)).
                allowedLateness(Time.seconds(2)).
                sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sensor") {});
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windows.sum(1);
        sum.print("sum");
        sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sensor") {}).print("sideOutput");
        env.execute();
    }
}
