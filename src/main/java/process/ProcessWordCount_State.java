package process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//todo
public class ProcessWordCount_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //将输出变为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsTuple = socketDS.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        //按照word分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = wordsTuple.keyBy(0);
        //聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = tuple2TupleKeyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //报错 :类加载时还没上下文,所以写open里面
            //ValueState<Integer> countState= getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count-state", Integer.class));

            ValueState<Integer> countState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //赋初始值,默认是null
                 countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count-state", Integer.class,0));
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //获取状态中的值
                Integer count = countState.value();


                out.collect(new Tuple2<>(value.f0,count +1));
                //更新状态
                countState.update(count+1);
            }
        });
        process.print();
        env.execute();
    }
}
