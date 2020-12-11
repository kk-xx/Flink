package sink;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class ToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDs = env.socketTextStream("hadoop102", 9999);
        //3.将数据写入Redis
        //设置redis配置
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        DataStreamSink<String> stringDataStreamSink = socketDs.addSink(new RedisSink<>(redisConf,new MyredisMapper() ));
        env.execute();
    }

    private static class MyredisMapper implements RedisMapper<String> {
        //指定写入数据类型,如果使用的是Hash或者ZSet,需要额外指定外层Key
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }
        //指定Redis中Key值(如果是Hash,指定的则是Field)
        @Override
        public String getKeyFromData(String s) {
            return s.split(",")[0];
        }
        //指定Redis中Value值
        @Override
        public String getValueFromData(String s) {
            return s.split(",")[1];
        }
    }
}
