package wordcount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//TODO  批处理wordcount
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        DataSet<String> input = env.readTextFile("input");
        //压平操作
        FlatMapOperator<String, Tuple2<String, Integer>> wordsTuple = input.flatMap(new MyFlatMap());
        DataSet<Tuple2<String, Integer>> sum = wordsTuple.groupBy(0).sum(1);

        sum.print();

    }
    //内部内实现FlatMapFunction

}

