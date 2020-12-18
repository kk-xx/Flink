package test;

import bean.NewSensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

//todo 从Kafka读取传感器数据,统计每个传感器发送温度的次数存入MySQL(a表),如果某个传感器温度连续10秒不下降,则输出报警信息到侧输出流并存入MySQL(b表).
//非常完美
//思路: 用process函数处理数据,普通信息正常输出,输出到mysql a 表;报警信息侧输出流输出 ,输出到mysql  b 表
public class process_MysqlAToB {
    public static void main(String[] args) throws Exception {
        // kafka配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从kafka读取数据
        DataStreamSource kfkDS = env.addSource(new FlinkKafkaConsumer011("sensor", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<NewSensorReading> map = kfkDS.map(new mapSensor());
        KeyedStream<NewSensorReading, String> keyDS = map.keyBy(NewSensorReading::getId);
        SingleOutputStreamOperator<String> processDS = keyDS.process(new twoCaseProcess(10));
        //a 表数据
        processDS.addSink(new MyJdbc("insert into a (id,count) values(?,?) on duplicate key update count=?;", 3));
        processDS.getSideOutput(new OutputTag<String>("DescTemp") {
        }).addSink(new MyJdbc("insert into b (id,ts) values(?,?)", 2));
        //打印测试
        processDS.print("次数");
        processDS.getSideOutput(new OutputTag<String>("DescTemp") {
        }).print("温差");
        env.execute();

    }

    //1.map处理函数
    private static class mapSensor implements MapFunction<String, NewSensorReading> {

        @Override
        public NewSensorReading map(String value) throws Exception {
            String[] split = value.split(",");
            return new NewSensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }
    }

    //2.process处理函数
    private static class twoCaseProcess extends KeyedProcessFunction<String, NewSensorReading, String> {
        private int time;

        public twoCaseProcess(int time) {
            this.time = time;
        }

        //初始化状态  温度,时间,次数
        private ValueState<Double> temp_stat;
        private ValueState<Long> time_stat;
        private ValueState<Long> count_stat;

        //获取状态
        @Override
        public void open(Configuration parameters) throws Exception {
            temp_stat = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp_stat", Double.class, Double.MIN_VALUE));
            time_stat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time_stat", Long.class));
            count_stat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count_stat", Long.class, 0L));
        }

        //对数据和状态做出处理
        @Override
        public void processElement(NewSensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取状态信息
            Double lastTemp = temp_stat.value();
            Long lastTime = time_stat.value();
            Long lastCount = count_stat.value();
            //1.每条数据都要干的事情
            //1.1获取当前数据的温度
            Double curTemp = value.getTemp();
            //1.2更新温度和次数
            temp_stat.update(curTemp);
            count_stat.update(lastCount + 1L);

            //因为一会还要将结果输出到mysql,所以先用字符串输出,这个相当于a表的输出
            out.collect(value.getId() + " " + count_stat.value());
            //下边得到报警信息
            if (curTemp > lastTemp && lastTime == null) {
                long curTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(curTime + time * 1000L);
                time_stat.update(curTime);
            } else if (curTemp < lastTemp && lastTime != null) {
                ctx.timerService().deleteProcessingTimeTimer(lastTime);
                time_stat.clear();
            }
        }

        //输出报警信息,这里的timestamp为定时器开始时间
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ctx.output(new OutputTag<String>("DescTemp") {
            }, ctx.getCurrentKey() + " " + timestamp + " 已经有" + time + "秒温度没有下降了");
            time_stat.clear();
        }
    }
    //连接mysql
    private static class MyJdbc extends RichSinkFunction<String> {
        private String sql;
        private int para;

        public MyJdbc(String sql, int para) {
            this.sql = sql;
            this.para = para;
        }

        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] words = value.split(" ");
            for (int i = 0; i < para; i++) {
                if (i == 2) {
                    preparedStatement.setObject(i + 1, words[i - 1]);
                } else {
                    preparedStatement.setObject(i + 1, words[i]);
                }
            }
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();

        }
    }
}
