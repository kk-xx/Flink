package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorReading;

import java.util.Collections;

//TODO 1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。2. Connect只能操作两个流，Union可以操作多个。
public class Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.readTextFile("input/2.txt");
        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String value) throws Exception {
                String[] s = value.split(" ");
                return new SensorReading(s[0], Long.parseLong(s[1]), Double.parseDouble(s[2]));
            }
        });
        //将原来的DataStream分为两个
        SplitStream<SensorReading> split = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                //集合实现了迭代器接口
                return value.getV() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        //select方法,选则一种
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> union = high.union(low);
        union.print("union");
        env.execute();
//union> SensorReading(sensor=sensor_2, l=1234567890, v=32.5)
//union> SensorReading(sensor=sensor_1, l=1234567981, v=33.5)
//union> SensorReading(sensor=sensor_3, l=1234898123, v=31.5)
//union> SensorReading(sensor=sensor_3, l=1234568911, v=29.5)
//union> SensorReading(sensor=sensor_2, l=1234855200, v=11.5)
//union> SensorReading(sensor=sensor_1, l=1234788888, v=29.5)
    }
}
