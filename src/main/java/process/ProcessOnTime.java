package process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//TODO  实现定时器 定时器只支持keyStream ,要先分组,实现接收到数据后5秒后触发定时器
public class ProcessOnTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //按进来的数据分组
        SingleOutputStreamOperator<Object> processOntime = socketDS.keyBy(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String value) throws Exception {
                return value;
            }
        }).process(new KeyedProcessFunction<Object, String, Object>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
                out.collect(value);
                //5秒后触发定时器

                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000L);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                System.out.println("定时器触发");
            }
        });
            processOntime.print();
            env.execute();
    }
}
