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
//todo 设置watermark(乱序数据) 2s,并开窗(滚动窗口) 5s
public class TumblingEventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sockDS = env.socketTextStream("hadoop102",9999);
        //设置时间为eventTime,默认为process(处理)时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置watermark字段
        SingleOutputStreamOperator<String> boundWater = sockDS.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] words = element.split(",");
                return Long.parseLong(words[1])*1000L;
            }
        });
        //将原始数据变为tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatDS = boundWater.flatMap(new MySensorFlatMap());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = flatDS.keyBy(0);
        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS = keyDS.timeWindow(Time.seconds(5));
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowDS.sum(1);
        sum.print();
        env.execute();
    }

    private static class MySensorFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] split = value.split(",");
            out.collect(new Tuple2<>(split[0],1));
        }
    }
}
