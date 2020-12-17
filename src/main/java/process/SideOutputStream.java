package process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
//TODO  侧输出流  实现温度小于30主流输出,温度大于30侧输出流输出
public class SideOutputStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        KeyedStream<String, Object> keyDS = socketDS.keyBy(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String value) throws Exception {

                return value;
            }
        });
        SingleOutputStreamOperator<Object> process = keyDS.process(new KeyedProcessFunction<Object, String, Object>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
                String[] split = value.split(",");
                double temp = Double.parseDouble(split[2]);
                //如果温度小于30,就主流输出
                if (temp > 30) {
                    out.collect(value);
                    //如果温度大于30就从侧输出流输出
                } else {
                    ctx.output(new OutputTag<Tuple2<String, Double>>("low") {
                    }, new Tuple2<String, Double>(split[0], temp));
                }
            }
        });
        process.print("high");
        //记得输出时侧输出流加{}
        process.getSideOutput(new OutputTag<Tuple2<String,Double>>("low"){}).print("low");
        env.execute();
    }
}
