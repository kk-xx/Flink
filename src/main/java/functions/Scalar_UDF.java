package functions;

import bean.NewSensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
//todo 自定义函数 udf 1 -> 1  获取字符串长度 (自己写的类要时public)
public class Scalar_UDF {
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
        tableEnv.registerFunction("mylen",new MyLength());

        //table API
        Table tableResult = table.select("id,id.mylen");

        //sql API
        //可以用从流获得表,记得加空格
        Table sqlResult = tableEnv.sqlQuery("select id,mylen(id) from " + table);
        tableEnv.toAppendStream(tableResult, Row.class).print("table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");
        env.execute();
    }

    public static class MyLength extends ScalarFunction {
        //这个方法必须自己写,而且函数名字必须为eval,返回值看结果
        public int eval(String field){
            return field.length();
        }
    }
}
