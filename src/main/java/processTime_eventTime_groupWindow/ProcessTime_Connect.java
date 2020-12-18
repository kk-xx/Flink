package processTime_eventTime_groupWindow;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
//todo 根据连接器得到处理时间
public class ProcessTime_Connect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                        //获取处理时间
                        .field("pt",DataTypes.TIMESTAMP(3))
                        .proctime())
                .createTemporaryTable("sensorTable");
        Table sensorTable = tableEnv.from("sensorTable");
        sensorTable.printSchema();
        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- temp: DOUBLE
        // |-- pt: TIMESTAMP(3)

    }
}
