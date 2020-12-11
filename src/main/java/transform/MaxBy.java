package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorReading;

//TODO 除了比较的值,其他的值也会跟着变   max :除了比较的值,其他的属性与第一条数据一样
public class MaxBy {
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
        KeyedStream<SensorReading, Tuple> sensor = map.keyBy("sensor");
        SingleOutputStreamOperator<SensorReading> max = sensor.maxBy("v");
        max.print();
        env.execute();
        //SensorReading(sensor=sensor_2, l=1234567890, v=32.5)
        //SensorReading(sensor=sensor_1, l=1234567981, v=33.5)
        //SensorReading(sensor=sensor_3, l=1234568911, v=29.5)
        //SensorReading(sensor=sensor_2, l=1234567890, v=32.5)
        //SensorReading(sensor=sensor_1, l=1234567981, v=33.5)
        //SensorReading(sensor=sensor_3, l=1234898123, v=31.5)
    }
}
