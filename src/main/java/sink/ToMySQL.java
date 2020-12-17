package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//todo 设置jdbc连接,连入mysql
public class ToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        socketDS.addSink(new MyJdbc());
        env.execute();
    }

    private static class MyJdbc extends RichSinkFunction<String> {
        //初始化连接
        private Connection connect;
        //初始化参数
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接
             connect = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
             //对于设置主键的就要设置on duplicate key update 更新的键 = ?,这样才能根据主键有则更新,无则插入
             preparedStatement = connect.prepareStatement("insert into sensor(id,temp) values(?,?) on duplicate key update temp = ?;");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //根据输入value输入参数
            String[] split = value.split(",");
            //参数下标从1开始
            preparedStatement.setString(1,split[0]);
            preparedStatement.setString(2,split[2]);
            preparedStatement.setString(3,split[2]);
            preparedStatement.execute();
        }
        //关闭连接
        @Override
        public void close() throws Exception {
            connect.close();
            preparedStatement.close();
        }
    }
}
