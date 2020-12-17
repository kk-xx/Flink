package process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import source.SensorReading;

//todo 用富函数实现 : 同一个组的两次温度相差10度,报警
public class RichTempDiff {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //将每行数据变为SensorReading样例类
        SingleOutputStreamOperator<SensorReading> map = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //按照sensorID分组
        KeyedStream<SensorReading, String> keyDS = map.keyBy(SensorReading::getSensor);
        //根据数据判断是否报警,用flatmap可以控制返回值的类型,可以不返回东西,为了让不报警时不做反应考虑
        SingleOutputStreamOperator<String> result = keyDS.flatMap(new myRichDiff(10.0));
        result.print();
        env.execute();
    }

    private static class myRichDiff extends RichFlatMapFunction<SensorReading,String> {
        private double maxDiff;
        private ValueState<Double> difftemp;
        public myRichDiff(double temp) {
            this.maxDiff=temp;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
             difftemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("difftemp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            //先把之前的状态拿出来,再更新,让状态一直是最新的,并且拿出之前的,让有的比
            Double stateTemp = difftemp.value();
            difftemp.update(value.getV());
            if (stateTemp!=null && (Math.abs(value.getV()-stateTemp)>maxDiff)){
                out.collect("温差大于"+maxDiff+"度了,可能有问题");
            }
        }
    }
}
