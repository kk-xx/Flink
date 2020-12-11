package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<Object> map = input.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                return value.length();

            }
        });
        map.print();
        env.execute();

    }
}
