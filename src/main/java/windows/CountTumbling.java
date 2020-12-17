package windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
//todo countwindow只有相同key的数据来够窗口条数才会触发计算
public class CountTumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDS = env.readTextFile("input/1.txt");
        //变为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordTuple = textDS.flatMap(new WordTuple());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = wordTuple.keyBy(0);
        //设置数据个数滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> tumblingWindow = keyDS.countWindow(5);
        //窗口内聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tumblingWindow.sum(1);
        sum.print();
        env.execute();
    }

    private static class WordTuple implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
