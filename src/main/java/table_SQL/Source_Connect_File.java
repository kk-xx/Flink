package table_SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
//todo 从file中读取的数据类型为old的csv
public class Source_Connect_File {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置文件系统连接器
        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE())
                ).createTemporaryTable("sensorTable");
        //用SQLAPI可以直接查
        Table table = tableEnv.sqlQuery("select id,max(temp) from sensorTable group by id");
        //tableAPI先创建表
        Table fileTable = tableEnv.from("sensorTable");
        Table tableResult = fileTable.select("id,temp").where("id ='sensor_1'");
        //更新输出
        tableEnv.toRetractStream(table, Row.class).print("SQL");
        //追加输出
        tableEnv.toAppendStream(tableResult,Row.class).print("table");
        env.execute();
    }
}
