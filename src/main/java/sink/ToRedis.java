package sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//todo 将数据写入redis的hash数据结构
public class ToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDs = env.socketTextStream("hadoop102", 9999);
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        socketDs.addSink(new RedisSink<>(redisConf,new MyRedisFunc()));
        env.execute();
    }

    private static class MyRedisFunc implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            //指定写入的数据类型,如果是hset或者zset,还要指定外层key
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor");
        }
        //指定key,hash的相同key不同值会更新
        @Override
        public String getKeyFromData(String data) {
            String[] split = data.split(",");
            return split[0];
        }
        //指定value
        @Override
        public String getValueFromData(String data) {
            String[] split = data.split(",");
            return split[2];
        }
    }
}

