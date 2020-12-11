package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest3_MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> myDataStream = env.addSource(new MySensor());
        myDataStream.print();
        env.execute();
    }

    //继承SourceFunction接口,重写run与cancel方法,输出SensorReading类对象
    private static class MySensor implements SourceFunction<SensorReading> {
        private boolean running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0;i<10;i++){
                map.put("sensor_"+(i+1),random.nextGaussian()*20);
            }
        while (running){
            for (String sensorId : map.keySet()) {
                double v = map.get(sensorId) + random.nextGaussian();
                map.put(sensorId,v);
                ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),v));
            }
        }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
