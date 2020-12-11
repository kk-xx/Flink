package transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<Object> flatMap = input.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        flatMap.print();
        env.execute();

        //wordcount
//        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator =
//                input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] s = value.split(" ");
//                for (String s1 : s) {
//                    out.collect(new Tuple2<>(s1,1));
//                }
//            }});
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2SingleOutputStreamOperator.keyBy(0).sum(1);
//        sum.print();
//        env.execute();

//        SingleOutputStreamOperator<Object> sum = flatMap.keyBy(0).sum(1);

        }
    }
