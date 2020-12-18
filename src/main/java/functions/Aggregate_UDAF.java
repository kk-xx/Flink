package functions;

import bean.NewSensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
//todo 自定义udaf函数,求出每个id的温度平均数
public class Aggregate_UDAF {
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
        tableEnv.registerFunction("tempAvg", new TempAvg());

        //5.TableAPI 使用UDAF
        Table tableResult = table.groupBy("id")
                .select("id,temp.tempAvg");

        //6.SQL 方式使用UDAF
        Table sqlResult = tableEnv.sqlQuery("select id,tempAvg(temp) from " + table + " group by id");

        //7.转换为流进行打印数据
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");

        //8.执行
        env.execute();

    }

    //求平均数函数
    //参数为输入 和 缓冲区数据类型
    public static class TempAvg extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        //初始化 缓冲数据
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        //每条数据过来怎么计算
        public void accumulate(Tuple2<Double, Integer> acc, Double result) {
            acc.f0 += result;
            acc.f1 += 1;
        }

        //最终计算结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }


    }
}
