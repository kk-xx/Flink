package wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置连接服务器的hostname和端口
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsTuple = input.flatMap(new MyFlatMap()).setParallelism(2);
        //分组,求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordsTuple.keyBy(0).sum(1);
        //打印结果
        sum.print();
//        env.setParallelism(3); 设置并行度
        //运行任务
        env.execute();

    }
}
