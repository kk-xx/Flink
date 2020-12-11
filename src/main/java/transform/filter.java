package transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<String> hello = input.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                return value.contains("hello");
            }
        });
        hello.print();
        env.execute();
    }
}
