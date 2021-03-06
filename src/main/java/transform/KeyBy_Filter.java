package transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorReading;

public class KeyBy_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readTextFile("input/2.txt");
        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String value) throws Exception {
                String[] s = value.split(" ");
                return new SensorReading(s[0], Long.parseLong(s[1]), Double.parseDouble(s[2]));
            }
        });
        //分组
        KeyedStream<SensorReading, Tuple> sensor = map.keyBy("sensor");
        //过滤
        SingleOutputStreamOperator<SensorReading> filter = sensor.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.getV() > 30.0;
            }
        });
        filter.print();
        env.execute();
        //2> SensorReading(sensor=sensor_2, l=1234567890, v=32.5)
        //5> SensorReading(sensor=sensor_1, l=1234567981, v=33.5)
        //1> SensorReading(sensor=sensor_3, l=1234898123, v=31.5)
    }
}
