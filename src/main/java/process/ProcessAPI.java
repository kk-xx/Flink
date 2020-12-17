package process;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//侧输出流,定时器 用process,
//第三方框架,状态编程 就rich(富函数)
public class ProcessAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Object> process = socketDS.process(new ProcessFunction<String, Object>() {
            //声明周期方法
            @Override
            public void open(Configuration parameters) throws Exception {
                //获取运行上下文,做状态编程
                RuntimeContext runtimeContext = getRuntimeContext();

            }

            //处理进入系统的每一条数据, out为主输出, ctx为侧输出流
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
                //输出数据
                out.collect("");
                //获取时间
                //获取处理时间相关数据和注册和删除定时器
                ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(1L);
                ctx.timerService().deleteProcessingTimeTimer(1L);
                //获取事件时间相关数据和注册和删除定时器
                ctx.timerService().currentWatermark();
                ctx.timerService().registerEventTimeTimer(1L);
                ctx.timerService().deleteEventTimeTimer(1L);
                //侧输出流
                ctx.output(new OutputTag<String>("outputTagName"){},value);
            }

            //定时器触发任务执行
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {


            }

            //声明周期方法
            @Override
            public void close() throws Exception {


            }
        });
        process.getSideOutput(new OutputTag<String>("outputTagName"));
    }
}
