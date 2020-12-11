package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import source.SensorReading;

import java.util.Collections;

//TODO connect 可以连接数据类型不同的dataStream,输出时map方法实现CoMapFunction接口,定义每个dataStream的输出形式,但是只能连接两个流
public class Connect_CoMap {
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
        //将一种输出变为tuple
        SingleOutputStreamOperator<Tuple2<String, Double>> highSOS = split.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getSensor(), value.getV());
            }
        });
        //将一种输出变为类对象
        DataStream<SensorReading> low = split.select("low");
        //将两种connect
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = highSOS.connect(low);
        //实现CoMapFunction接口
        SingleOutputStreamOperator<Object> maps = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return value;
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return value;
            }
        });
        maps.print("maps");
        env.execute();
//maps> (sensor_2,32.5)
//maps> SensorReading(sensor=sensor_3, l=1234568911, v=29.5)
//maps> (sensor_1,33.5)
//maps> SensorReading(sensor=sensor_2, l=1234855200, v=11.5)
//maps> (sensor_3,31.5)
//maps> SensorReading(sensor=sensor_1, l=1234788888, v=29.5)
    }
}
