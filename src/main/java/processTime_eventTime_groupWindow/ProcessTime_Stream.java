package processTime_eventTime_groupWindow;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
//todo 根据流获取处理时间
public class ProcessTime_Stream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        //对流中数据进行样例类处理
        SingleOutputStreamOperator<NewSensorReading> mapDS = socketDS.map(new MapFunction<String, NewSensorReading>() {
            @Override
            public NewSensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //将流转换为表,并指定时间字段为处理时间,pt为随便起的名字
        Table table = tableEnv.fromDataStream(mapDS, "id,ts,temp,pt.proctime");
        table.printSchema();
        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- temp: DOUBLE
        // |-- pt: TIMESTAMP(3) *PROCTIME*
    }
}
