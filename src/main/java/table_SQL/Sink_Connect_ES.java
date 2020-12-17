package table_SQL;

import bean.NewSensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class Sink_Connect_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<NewSensorReading> socketDS = env.socketTextStream("hadoop102", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(socketDS);
        Table tableResult = table.select("id,temp");
//        tableEnv.createTemporaryView("sensor",socketDS);
//        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");
        tableEnv.connect(
                new Elasticsearch()
                        .version("6")
                        .host("hadoop102",9200,"http")
                        .index("mysensor")
                        .documentType("_doc")
                        //执行一条显示一条
                        .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                //追加模式,必须指定模式
                .inAppendMode()
                .createTemporaryTable("es");

        tableEnv.toAppendStream(tableResult, Row.class).print();
        tableEnv.insertInto("es",tableResult);
//        tableEnv.insertInto("es",sqlResult);
        env.execute();
    }
}
