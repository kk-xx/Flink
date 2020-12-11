package wordcount;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//TODO 有界流处理wordcount
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //创建流的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取读取文件路径
        DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsTuple = input.flatMap(new MyFlatMap());
        //分组是 keyBy
        KeyedStream<Tuple2<String, Integer>, Tuple> keyvalue = wordsTuple.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyvalue.sum(1);
        //打印结果
        sum.print().setParallelism(1);

        //启动任务
        env.execute("Flink02_WordCount_Bounded");

    }
}
