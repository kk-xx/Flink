package processTime_eventTime_groupWindow;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

//todo 根据连接器得到事件时间 注意 : 这种用不了
public class EventTime_Connect {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        //获取事件时间
                        .rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(1000))
                        .field("temp",DataTypes.DOUBLE())).createTemporaryTable("sensorTable");
        //读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");
        //打印表信息
        sensorTable.printSchema();
        //root
        // |-- id: STRING
        // |-- temp: DOUBLE

    }
}
