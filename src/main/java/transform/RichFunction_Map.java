package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorReading;

import java.util.Collections;

//TODO Rich Function有一个生命周期的概念。典型的生命周期方法有：
//open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
//close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
//getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
public class RichFunction_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setParallelism(1);
        DataStreamSource<String> input = env.readTextFile("input/2.txt");
        SingleOutputStreamOperator<SensorReading> mapDs = input.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String value) throws Exception {
                String[] s = value.split(" ");
                return new SensorReading(s[0], Long.parseLong(s[1]), Double.parseDouble(s[2]));
            }
        });
        SingleOutputStreamOperator<Tuple2<Integer, String>> richmap = mapDs.map(new RichMapFunction<SensorReading, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(SensorReading value) throws Exception {
                return new Tuple2<Integer, String>(getRuntimeContext().getIndexOfThisSubtask(), value.getSensor());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("my map open");
            }

            @Override
            public void close() throws Exception {
                System.out.println("my map close");
            }
        });


        richmap.print();
        env.execute();
    }
}
