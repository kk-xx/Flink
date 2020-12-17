package test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.HashSet;

//todo 让文件的每个单词只出现一次

public class SetWordOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketDs = env.readTextFile("input/1.txt");
        SingleOutputStreamOperator<String> flatmapDS = socketDs.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        HashSet<String> set = new HashSet<>();
        flatmapDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (set.contains(value)){
                    return false;
                }else {
                    set.add(value);
                    return true;
                }
            }
        }).print();

        env.execute();
    }
}
