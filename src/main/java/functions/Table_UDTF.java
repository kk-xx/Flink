package functions;

import bean.NewSensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

//todo 自定义的 udtf函数 1->多  以下划线为分隔符 sensor_1  =>  [sensor 6,1 1]
public class Table_UDTF {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<NewSensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new NewSensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);
        //4.注册函数
        tableEnv.registerFunction("spilt", new MySpilt());

        //5.tableAPI
        Table tableResult = table.joinLateral("spilt(id) as (word,len)").select("id,word,len");
        //6.sqlAPI
        Table sqlResult = tableEnv.sqlQuery("select id,word,len from " + table + ",lateral table(spilt(id)) as kk(word,len)");


        //7.转换为流进行打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        //8.执行
        env.execute();
    }

    //定义泛型,输出类型
    public static class MySpilt extends TableFunction<Tuple2<String,Integer>> {
        //写 eval 方法
        public void eval(String field){
            String[] spilt = field.split("_");
            for (String word : spilt) {
                collect(new Tuple2<>(word,word.length()));
            }
        }
    }
}
