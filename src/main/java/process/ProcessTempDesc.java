package process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import source.SensorReading;

//todo 监控温度传感器的温度值,如果温度值在10秒钟之内(processing time)没有下降,则报警  (可以定时器实现)

public class ProcessTempDesc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> map = socketDS.map(new MySensor());
        KeyedStream<SensorReading, String> keyDS = map.keyBy(SensorReading::getSensor);
        SingleOutputStreamOperator<String> process = keyDS.process(new KeyFunc(10));
        process.print();
        env.execute();
    }

    private static class MySensor implements MapFunction<String, SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] split = value.split(",");
            return new SensorReading(split[0],Long.parseLong(split[1]),Double.parseDouble(split[2]));
        }
    }

    private static class KeyFunc extends KeyedProcessFunction<String,SensorReading,String> {
        private int time; //时间间隔
        private ValueState<Double> tempState;
        private ValueState<Long> timeState;

        public KeyFunc(int i) {
            this.time=i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //默认值为Double.MIN_VALUE
             tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("desc-temp", Double.class, Double.MIN_VALUE));
             timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //todo 这里是必须执行的操作

            //获取上一次温度值
            Double lastTemp = tempState.value();
            //获取时间状态
            Long lastTime = timeState.value();
            //获取当前温度
            Double temp = value.getV();
            //更新温度
            tempState.update(temp);

            //todo 实际这里的timestate(时间状态)就是定时器是否存在,所以判断中除了判断条件还需要判断定时器存在与否,然后逻辑中生成或者删除定时器

            //注册定时器时机: 温度出现上升,并且时间为状态null
            if (temp>lastTemp && lastTime==null){
                //注册以现在为时间的往后10秒钟定时器,10秒为构造器参数
                long curTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(curTime+time*1000L);
                //更新时间状态
                timeState.update(curTime);
            }
            //判断删除定时器时机 : 温度下降 并且时间状态不为null(代表不是连续下降)
            else if(temp<lastTemp && lastTime != null){
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(lastTime);
                //清空时间状态
                timeState.clear();
            }
        }
        //定时器触发任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //输出数据
            System.out.println(ctx.getCurrentKey()+"连续"+time+"秒没有出现温度下降");
            //清空时间状态
            timeState.clear();
        }
    }
}
