package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class Sink_Connect_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<NewSensorReading> socketDS = env.socketTextStream("hadoop102", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(socketDS);
        //不能写upsert的语句,文件只支持追加
        //Table tableResult = table.groupBy("id").select("id,max(temp)");
        tableEnv.createTemporaryView("sensor",socketDS);
        Table SQLResult = tableEnv.sqlQuery("select id,temp from sensor");
        //创建文件系统的连接器
        tableEnv.connect(new FileSystem().path("output"))
                .withFormat(new OldCsv())
                .withSchema(new Schema().field("id", DataTypes.STRING()).field("max",DataTypes.DOUBLE()))
                .createTemporaryTable("maxtable");
//        tableEnv.insertInto("maxtable",tableResult);
        tableEnv.insertInto("maxtable",SQLResult);

        env.execute();
    }
}
